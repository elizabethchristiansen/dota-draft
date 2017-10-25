import time
import logging
import aiohttp
import asyncio
import requests
import queue

from functools import partial
from ._errors import ServiceNotAvailable, InvalidAuthKey, RateLimitActive, CouldNotInit, OAPIError

class API( object ):
	def __init__( self, key, data_format = "json", lang = "en" ):
		self.key = key
		self.format = data_format
		self.lang = lang

		self.base_dota_url = "https://api.steampowered.com/IDOTA2Match_570/"
		self.base_oapi_url = "https://api.opendota.com/api/"

		self.base_headers = {
			"Content-Type": "application/x-www-form-urlencoded"
		}
		self.base_payload = {
			"key":			self.key,
			"format":		self.format,
			"language":		self.lang
		}

		self.dota_api_timers = {
			"last_request":			0,
			"wait_seconds":			5,
			"rate_limit_wait":		30,
			"empty_wait_seconds": 	15	
		}

		self.open_api_timers = {
			"last_request":		0,
			"wait_seconds":		0.35,
			"rate_limit_wait":	60,
			"404_sleep":		60,
		}

		self.dropped_games = 0

		self._get_current_seq_num()

		self.events = asyncio.get_event_loop()
		self.matches_queue = asyncio.Queue( maxsize = 200 )
		self.match_info_queue = queue.Queue()

		self.oapi_lock = asyncio.Lock()

	def _get_current_seq_num( self ):
		payload = { "matches_requested": 1 }
		headers = self.base_headers
		url = self.base_dota_url + "GetMatchHistory/v1/"

		payload.update( self.base_payload )

		r = requests.get( url, headers = headers, params = payload )
		j = r.json()

		self.dota_api_timers["last_request"] = time.time()

		if r.status_code == 200:
			j = r.json()["result"]["matches"][0]
			self.seq_from = int( j["match_seq_num"] )
			logging.info( "Found the first seq num from the Dota API ({})".format( self.seq_from ) )
		else:
			logging.error( "Could not initialize the Dota API parser (could not get seq num, status_code code: {}), exiting".format( r.status_code ) )
			raise CouldNotInit

	def _parse_match_history( self, data ):
		valid_matches = []

		for i in data["result"]["matches"]:
			valid = True
			players = i["players"]

			for p in players:
				try:
					if p["leaver_status"] != 0 and p["leaver_status"] != 1:
						valid = False
						break
				except KeyError:
					valid = False
					break

			if i["lobby_type"] != 7 or i["human_players"] != 10 or i["game_mode"] != 22:
				valid = False

			if valid:
				valid_matches.append( i["match_id"] )
			else:
				self.dropped_games += 1

		return valid_matches 

	async def _get_matches( self ):
		while True:
			if time.time() - self.dota_api_timers["last_request"] < self.dota_api_timers["wait_seconds"]:
				await asyncio.sleep( self.dota_api_timers["wait_seconds"] - ( time.time() - self.dota_api_timers["last_request"] ) )

			requested = 100
			headers = self.base_headers
			payload = { "start_at_match_seq_num": self.seq_from, "matches_requested": requested }
			payload.update( self.base_payload )
			url = self.base_dota_url + "GetMatchHistoryBySequenceNum/v1/"

			get_func = partial( requests.get, url, headers = headers, params = payload )
			future_res = self.events.run_in_executor( None, get_func )
			res = await future_res
			self.dota_api_timers["last_request"] = time.time()	

			if res.status_code == 429:
				logging.warning( "We are being rate limited, waiting for {} seconds".format( self.rate_limit_wait ) )
				await asyncio.sleep( self.rate_limit_wait )
			elif res.status_code == 503 or res.status_code == 500:
				logging.error( "The API is down or otherwise not responding, exiting!" )
				raise ServiceNotAvailable
			elif res.status_code == 401 or res.status_code == 403:
				logging.error( "Our authentication key seems to be wrong or we have otherwise been blocked from the service, exiting!" )
				raise InvalidAuthKey
			elif res.status_code == 200:
				logging.info( "Retrieved from Dota API URL {}".format( res.url ) )
				data = res.json()

				num_results = len( data["result"]["matches"] )
				if num_results > 0:
					self.seq_from += min( num_results, requested )
				else:
					logging.info( "We are going faster than the Dota API, waiting for {} seconds".format( self.dota_api_timers["empty_wait_seconds"] ) )
					await asyncio.sleep( self.dota_api_timers["empty_wait_seconds"] )

			valid_matches = self._parse_match_history( data )

			for i in valid_matches:
				await self.matches_queue.put( i )

	def _parse_match( self, data ):
		try:
			match_id = data["match_id"]
			dire_score = data["dire_score"]
			rad_score = data["radiant_score"]
			duration = data["duration"]
			winner = int( data["radiant_win"] )
			start = data["start_time"]
			region = data["region"]
			skill = data["skill"]

			game_mode = data["game_mode"]
			players = data["human_players"]
			lobby = data["lobby_type"]
		except ( KeyError, TypeError ) as e:
			logging.error( "The OAPI returned JSON which did not contain the necessary fields" )
			return None

		if game_mode != 22 or lobby != 7 or players != 10 or skill is None:
			return None

		try:
			salt = data["replay_salt"]
			replay = data["replay_url"]
			throw = data["throw"]
			loss = data["loss"]
		except KeyError:
			salt = "NULL"
			replay = "NULL"
			throw = "NULL"
			loss = "NULL"
		else:
			logging.info( "We found a game with replay data!" )

		dire_picks = []
		rad_picks = []
		for i in data["players"]:
			hero = i["hero_id"]
			team = int( format( i["player_slot"], "08b" )[0] )

			if team == 1:
				dire_picks.append( hero )
			else:
				rad_picks.append( hero )

		if len( dire_picks ) != 5 or len( rad_picks ) != 5:
			return None

		match_details = {
			"match_id":			match_id,
			"match_time":		start,
			"winner":			winner,
			"duration":			duration,
			"radiant_score":	rad_score,
			"radiant_picks":	rad_picks,
			"dire_score":		dire_score,
			"dire_picks":		dire_picks,
			"skill":			skill,
			"region":			region,
			"salt":				salt,
			"replay":			replay,
			"throw":			throw,
			"loss":				loss,
		}

		return match_details

	async def _oapi_request( self, url ):
		with await self.oapi_lock:
			if time.time() - self.open_api_timers["last_request"] < self.open_api_timers["wait_seconds"]:
					await asyncio.sleep( self.open_api_timers["wait_seconds"] - ( time.time() - self.open_api_timers["last_request"] ) )

			future_res = self.events.run_in_executor( None, requests.get, url )
			self.open_api_timers["last_request"] = time.time()
			logging.info( "Submitting request to OAPI URL {}".format( url ) )
		
		res = await future_res
		return res

	async def _get_matches_info( self ):
		while True:
			match_id = await self.matches_queue.get()
			url = self.base_oapi_url + "matches/" + str( match_id )

			retries = 5
			for i in range( 0, retries ):
				res = await self._oapi_request( url )

				if res.status_code == 404:
					logging.warning( "Match {} ({}) does not yet exist in the OAPI database, sleeping".format( match_id, res.url ) )
					await asyncio.sleep( self.open_api_timers["404_sleep"] )
					continue
				elif res.status_code != 200:
					logging.error( "There was an undefined error in the OAPI call to {} (status code: {}), sleeping in case it is rate limiting".format( res.url, res.status_code ) )
					await asyncio.sleep( self.open_api_timers["rate_limit_wait"] )
					continue #raise OAPIError

				break

			if res.status_code != 200:
				logging.error( "Match {} did not appear in the OAPI database after {} retries (status code {}), skipping to next match".format( match_id, retries, res.status_code ) )
				continue

			data = res.json()
			match = self._parse_match( data )

			if match is not None:
				self.match_info_queue.put( match )
			else:
				self.dropped_games += 1

	def get_match( self ):
		return self.match_info_queue.get()

	def run( self ):
		self.events.create_task( self._get_matches() )
		self.events.create_task( self._get_matches_info() )
		self.events.create_task( self._get_matches_info() )
		self.events.run_forever()
