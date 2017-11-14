import time
import logging
import asyncio
import requests
import queue

from functools import partial
from threading import Semaphore
from ._errors import ServiceNotAvailable, InvalidAuthKey, RateLimitActive, CouldNotInit, OAPIError


class API( object ):
	def __init__( self, key, retry_on_error = True, max_retry = 5, data_format = "json", lang = "en" ):
		self.key = key
		self.format = data_format
		self.lang = lang
		self.retry = retry_on_error
		self.max_retry = max_retry

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
			"last_request":				0,
			"wait_seconds":				5,
			"rate_limit_wait":			30,
			"rate_limit_wait_base":		30,
			"empty_wait_seconds": 		30,
			"queue_warning":			600,
			"continued_error_sleep":	600,
			"heartbeat":				0
		}

		self.num_oapi_threads = 2
		self.open_api_timers = {
			"last_request":				0,
			"wait_seconds":				0.4,
			"rate_limit_wait":			30,
			"rate_limit_wait_base":		30,
			"404_sleep":				60,
			"queue_warning":			600,
			"heartbeat":				[0] * self.num_oapi_threads
		}

		self.wait_increment = 20

		self._get_current_seq_num()

		self.events = asyncio.get_event_loop()
		self.matches_queue = asyncio.Queue( maxsize = 100000 )
		self.match_info_queue = queue.Queue( maxsize = 1000 )

		self.oapi_lock = asyncio.Lock()
		self.dotaapi_lock = asyncio.Lock()
		self.processes = Semaphore( value = self.num_oapi_threads + 1 )

		self.exit = False

	def close( self ):
		self.exit = True
		for i in range( 0, self.num_oapi_threads + 1 ):
			self.processes.acquire()

	def _get_current_seq_num( self ):
		payload = { "matches_requested": 1 }
		headers = self.base_headers
		url = self.base_dota_url + "GetMatchHistory/v1/"

		payload.update( self.base_payload )

		for _ in range( 0, self.max_retry ):
			r = requests.get( url, headers = headers, params = payload )

			if r.status_code != 200:
				logging.warning( "The initial sequence num query returned a non-200 status code ({}), retrying".format( r.status_code ) )
				time.sleep( self.dota_api_timers["rate_limit_wait"] )
				continue

			break
		else:
			logging.error( "Could not initialize the Dota API parser (could not get seq num, status_code code: {})".format( r.status_code ) )
			raise CouldNotInit

		self.dota_api_timers["last_request"] = time.time()

		j = r.json()["result"]["matches"][0]
		self.seq_from = int( j["match_seq_num"] )
		logging.info( "Found the first seq num from the Dota API ({})".format( self.seq_from ) )

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

		return valid_matches

	async def _dapi_request( self, url, headers, payload ):
		with await self.dotaapi_lock:
			if time.time() - self.dota_api_timers["last_request"] < self.dota_api_timers["wait_seconds"]:
				await asyncio.sleep( self.dota_api_timers["wait_seconds"] - ( time.time() - self.dota_api_timers["last_request"] ) )

			get_func = partial( requests.get, url, headers = headers, params = payload )
			future_res = self.events.run_in_executor( None, get_func )
			self.dota_api_timers["last_request"] = time.time()
			logging.info( "Submitting request to Dota API URL {}".format( url ) )
			res = await future_res

		return res

	async def _get_matches( self ):
		self.processes.acquire()

		while True:
			if self.exit:
				logging.status( "Dota API poller exited!" )
				self.processes.release()
				break

			try:
				if ( time.time() - self.dota_api_timers["heartbeat"] ) >= 3600:
					logging.status( "[Dota API] I'm still alive! Queue has {} items.".format( self.matches_queue.qsize() ) )
					self.dota_api_timers["heartbeat"] = time.time()

				requested = 100
				headers = self.base_headers
				payload = { "start_at_match_seq_num": self.seq_from, "matches_requested": requested }
				payload.update( self.base_payload )
				url = self.base_dota_url + "GetMatchHistoryBySequenceNum/v1/"

				for _ in range( 0, self.max_retry ):
					res = await self._dapi_request( url, headers, payload )

					if res.status_code != 200:
						if res.status_code == 429:
							logging.warning( "We are being rate limited on the Dota API, waiting for {} seconds".format( self.dota_api_timers["rate_limit_wait"] ) )
						elif res.status_code == 503 or res.status_code == 500:
							logging.error( "The Dota API is down or otherwise not responding, waiting for {} seconds".format( self.dota_api_timers["rate_limit_wait"] ) )
							if not self.retry:
								raise ServiceNotAvailable
						elif res.status_code == 401 or res.status_code == 403:
							logging.error( "Our Dota API authentication key seems to be wrong or we have otherwise been blocked from the service, waiting for {} seconds".format( self.dota_api_timers["rate_limit_wait"] ) )
							if not self.retry:
								raise InvalidAuthKey

						await asyncio.sleep( self.dota_api_timers["rate_limit_wait"] )
						self.dota_api_timers["rate_limit_wait"] += self.wait_increment
						continue
					else:
						logging.info( "Retrieved from Dota API URL {}".format( res.url ) )
						data = res.json()

						num_results = len( data["result"]["matches"] )
						if num_results > 0:
							self.seq_from += min( num_results, requested )
						else:
							logging.info( "We are going faster than the Dota API, waiting for {} seconds".format( self.dota_api_timers["empty_wait_seconds"] ) )
							await asyncio.sleep( self.dota_api_timers["empty_wait_seconds"] )
							continue

						break
				else:
					logging.error( "Could not poll the Dota API after {} retries. [URL: {}, status code: {}]".format( self.max_retry, res.url, res.status_code ) )
					if not self.retry:
						raise ServiceNotAvailable

					await asyncio.sleep( self.dota_api_timers["continued_error_sleep"] )
					logging.status( "Dota API thread woke up after previous errors (slept for {} seconds)".format( self.dota_api_timers["continued_error_sleep"] ) )
					continue

				self.dota_api_timers["rate_limit_wait"] = max( self.dota_api_timers["rate_limit_wait_base"], self.dota_api_timers["rate_limit_wait"] - self.wait_increment )
				valid_matches = self._parse_match_history( data )

				for i in valid_matches:
					while True:
						try:
							await asyncio.wait_for( self.matches_queue.put( i ), self.dota_api_timers["queue_warning"] )
						except asyncio.TimeoutError:
							logging.warning( "The asyncio match queue has been full for {} seconds [Dota API can't put]!".format( self.dota_api_timers["full_queue_warning"] ) )
							continue

						break

			except BaseException as e:
				logging.exception( "We encountered a fatal error ({}) in the Dota API puller. Sleeping for a long time and trying again.".format( str( e ) ) )
				await asyncio.sleep( 1800 )
				logging.status( "Waking the Dota API puller after a fatal error sleep" )

	def _parse_match( self, data, url ):
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
			logging.error( "The OAPI URL {} returned JSON which did not contain the necessary fields".format( url ) )
			return None

		if game_mode != 22 or lobby != 7 or players != 10:
			return None

		if skill is None:
			skill = 0

		try:
			salt = data["replay_salt"]
			replay = data["replay_url"]
			throw = data["throw"]
			loss = data["loss"]
		except KeyError:
			salt = None
			replay = None
			throw = None
			loss = None
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

	async def _get_matches_info( self, tid = 0 ):
		tid_num = tid
		tid = "Instance-" + str( tid )
		self.processes.acquire()

		while True:
			if self.exit:
				logging.status( "OAPI {} poller exited".format( tid ) )
				self.processes.release()
				break

			try:
				if ( time.time() - self.open_api_timers["heartbeat"][tid_num - 1] ) >= 3600:
					logging.status( "[OAPI {}] I'm still alive! Queue has ~{} items.".format( tid, self.match_info_queue.qsize() ) )
					self.open_api_timers["heartbeat"][tid_num - 1] = time.time()

				try:
					match_id = await asyncio.wait_for( self.matches_queue.get(), self.open_api_timers["queue_warning"] )
				except asyncio.TimeoutError:
					logging.warning( "The asyncio queue has been empty for {} seconds [OAPI {} can't pull]!".format( self.open_api_timers["queue_warning"], tid ) )
					continue

				url = self.base_oapi_url + "matches/" + str( match_id )

				for _ in range( 0, self.max_retry ):
					res = await self._oapi_request( url )

					if res.status_code != 200:
						if res.status_code == 404:
							logging.warning( "Match {} ({}) does not yet exist in the OAPI database, {} is sleeping".format( match_id, res.url, tid ) )
							await asyncio.sleep( self.open_api_timers["404_sleep"] )
						elif res.status_code == 429:	# I am guessing this is rate limiting since it is the same status code as the dota api, could be wrong
							logging.warning( "We are being rate limited by the OAPI, {} is waiting for {} seconds for URL {}".format( tid, self.open_api_timers["rate_limit_wait"], res.url ) )
							await asyncio.sleep( self.open_api_timers["rate_limit_wait"] )
						else:
							logging.error( "There was an undefined error in the OAPI call to {} (status code: {}), {} is sleeping for {} seconds".format( res.url, res.status_code, tid, self.open_api_timers["rate_limit_wait"] ) )
							if not self.retry:
								raise OAPIError

							await asyncio.sleep( self.open_api_timers["rate_limit_wait"] )

						self.open_api_timers["rate_limit_wait"] += self.wait_increment
						continue

					break
				else:
					logging.error( "Match {} did not appear in the OAPI database after {} retries (status code {}), skipping to next match".format( match_id, self.max_retry, res.status_code ) )
					continue

				self.open_api_timers["rate_limit_wait"] = max( self.open_api_timers["rate_limit_wait_base"], self.open_api_timers["rate_limit_wait"] - self.wait_increment )
				data = res.json()
				match = self._parse_match( data, res.url )

				if match is not None:
					while True:
						try:
							self.match_info_queue.put( match, timeout = self.open_api_timers["queue_warning"] )
						except queue.Full:
							logging.warning( "The match queue has been full for {} seconds [OAPI {} can't put]!".format( self.open_api_timers["queue_warning"], tid ) )
							continue

						break

				self.matches_queue.task_done()

			except BaseException as e:
				logging.exception( "We encountered a fatal error ({}) in the OAPI {} puller. Sleeping for a long time and trying again.".format( str( e ), tid ) )
				await asyncio.sleep( 1800 )
				logging.status( "Waking the {} OAPI puller after a fatal error sleep".format( tid ) )

	def get_match( self ):
		while True:
			try:
				item = self.match_info_queue.get( timeout = self.open_api_timers["queue_warning"] )
			except queue.Empty:
				logging.warning( "The match queue has been empty for {} seconds [Database can't pull]!".format( self.open_api_timers["queue_warning"] ) )
				continue

			break

		self.match_info_queue.task_done()
		return item

	def run( self ):
		logging.info( "Initializing API poller event loop" )
		self.events.create_task( self._get_matches() )
		for i in range( 0, self.num_oapi_threads ):
			self.events.create_task( self._get_matches_info( tid = i ) )
		self.events.run_forever()
