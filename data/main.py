from dota2api import API

import sqlite3
import asyncio
import logging
import signal
import sys

logging.basicConfig( filename="scraper.log", filemode="a", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.WARNING )

def load_database():	
	db = sqlite3.connect( "database" )

	foreign_keys = ("PRAGMA foreign_keys = 1")

	create_table = '''CREATE TABLE IF NOT EXISTS match_info ( 
		match_id INTEGER PRIMARY KEY NOT NULL, 
		match_time INTEGER, 
		winner INTEGER, 
		duration INTEGER, 
		r_score INTEGER, 
		d_score INTEGER,
		skill INTEGER, 
		region INTEGER,
		salt INTEGER, 
		replay TEXT, 
		throw INTEGER, 
		loss INTEGER )'''

	create_picks_table = '''CREATE TABLE IF NOT EXISTS hero_picks ( 
		match_id INTEGER NOT NULL, 
		team INTEGER,
		hero INTEGER,
		PRIMARY KEY (match_id, hero),
		FOREIGN KEY (match_id) REFERENCES match_info(match_id) ON DELETE CASCADE )'''

	cursor = db.cursor()
	cursor.execute( foreign_keys )
	cursor.execute( create_table )
	cursor.execute( create_picks_table )
	db.commit()

	return db

def valid_game( game ):
	if type( game["match_id"] ) != int or game["match_id"] < 0:
		return False

	if type( game["match_time"] ) != int or game["match_time"] < 0:
		return False

	if type( game["winner"] ) != int or ( game["winner"] != 0 and game["winner"] != 1 ):
		return False

	if type( game["duration"] ) != int or game["duration"] <= 0:
		return False

	if type( game["radiant_score"] ) != int or game["radiant_score"] < 0:
		return False

	if type( game["dire_score"] ) != int or game["dire_score"] < 0:
		return False

	if type( game["skill"] ) != int or ( game["skill"] < 1 or game["skill"] > 3 ):
		return False

	if type( game["region"] ) != int or game["region"] < 0:
		return False

	if type( game["radiant_picks"] ) != list or len( game["radiant_picks"] ) != 5:
		return False

	if type( game["dire_picks"] ) != list or len( game["dire_picks"] ) != 5:
		return False

	if game["salt"] != "NULL" and type( game["salt"] ) != int:
		return False

	if game["throw"] != "NULL" and type( game["throw"] ) != int:
		return False

	if game["loss"] != "NULL" and type( game["loss"] ) != int:
		return False

	if type( game["replay"] ) != str or ( game["replay"] != "NULL" and game["replay"][0:4] != "http" ):
		return False

	return True

def exit_gracefully( signal, frame ):
	logging.error( "Caught SIGINT, closing loop and database gracefully!" )

	db.close()
	loop.stop()

	sys.exit(0)

if __name__ == "__main__":
	logging.info( "Initialized logging" )

	key = "413B7794E4797A6A070B473F904CA120"
	loop = asyncio.get_event_loop()

	api = API( key = key )
	future = loop.run_in_executor( None, api.run )

	db = load_database()
	cursor = db.cursor()

	signal.signal( signal.SIGINT, exit_gracefully )

	num_matches = 0
	while True:
		match = api.get_match()

		if not valid_game( match ):
			logging.warning( "We found an invalid game!\n{}\n".format( match ) )
			continue

		logging.info( "Found a valid game, committing to the database" )
		num_matches += 1

		match_query = "INSERT OR REPLACE INTO match_info VALUES ( {match_id}, {match_time}, {winner}, {duration}, {radiant_score}, {dire_score}, {skill}, {region}, {salt}, {replay}, {throw}, {loss} );".format( **match )
		cursor.execute( match_query )

		match_id = match["match_id"]

		for i in match["dire_picks"]:
			query = "INSERT OR REPLACE INTO hero_picks VALUES ( {}, {}, {} );".format( match_id, 0, i )
			cursor.execute( query )

		for i in match["radiant_picks"]:
			query = "INSERT OR REPLACE INTO hero_picks VALUES ( {}, {}, {} );".format( match_id, 1, i )
			cursor.execute( query )

		db.commit()

		if num_matches % 100 == 0:
			total = api.dropped_games + num_matches
			logging.info( "Kept {} games out of {}, {}%".format( num_matches, total, round( ( num_matches / total ) * 100.0, 2 ) ) )

