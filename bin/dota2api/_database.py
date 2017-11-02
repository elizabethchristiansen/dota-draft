import logging
import sqlite3
import tempfile
import os

from threading import Lock
from collections import defaultdict

class Database( object ):
    def __init__( self, database, mem_only = False ):
        self.database_dir = database
        self.in_memory = False
        self.mem_only = mem_only
        self.lock = Lock()

        logging.info( "Attempting to connect to {}".format( database ) )
        self._load_database()

        if mem_only:
            self.work_from_memory()

    def __enter__( self ):
        return self

    def __exit__( self, type, val, traceback ):
        if self.in_memory and not self.mem_only:
            self.work_from_file( overwrite_original = True )

        self.db.close()

    def __del__( self ):
        if self.in_memory and not self.mem_only:
            self.work_from_file( overwrite_original = True )

        self.db.close()

    def __iter__( self ):
        num_results = 1
        self.match_id_start = 0
        while num_results:
            max_id, num_results, match = self.get_drafts( starting_from = self.match_id_start, limit = 1, array = True )
            self.match_id_start = max_id + 1

            if num_results:
                yield ( match[0]["win_picks"], match[0]["loss_picks"] )

    def _load_database( self ):
        self.db = sqlite3.connect( self.database_dir )
        logging.info( "Connected to the database ({})".format( self.database_dir ) )

        foreign_keys = "PRAGMA foreign_keys = 1"

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

        cursor = self.db.cursor()
        cursor.execute( foreign_keys )
        cursor.execute( create_table )
        cursor.execute( create_picks_table )
        self.db.commit()

        logging.info( "Database initialization successful" )

    def _valid_game( self, game ):
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
        else:
            for i in game["radiant_picks"]:
                if type( i ) != int or ( i < 0 or i > 130 ):
                    return False

        if type( game["dire_picks"] ) != list or len( game["dire_picks"] ) != 5:
            return False
        else:
            for i in game["dire_picks"]:
                if type( i ) != int or ( i < 0 or i > 130 ):
                    return False

        if game["salt"] is not None and type( game["salt"] ) != int:
            return False

        if game["throw"] is not None and type( game["throw"] ) != int:
            return False

        if game["loss"] is not None and type( game["loss"] ) != int:
            return False

        if ( game["replay"] is not None and type( game["replay"] ) != str ) or ( type( game["replay"] ) == str and game["replay"][0:4] != "http" ):
            return False

        return True

    def work_from_memory( self ):
        if self.in_memory:
            logging.error( "Database is already in memory!" )
            return

        self.lock.acquire()
        logging.info( "Moving database to memory" )
        with tempfile.TemporaryFile( mode = "w+" ) as tmp:
            for line in self.db.iterdump():
                tmp.write( "{}\n".format( line ) )
            self.db.close()
            tmp.seek(0)

            self.db = sqlite3.connect( ":memory:" )
            self.db.cursor().executescript( tmp.read() )
            self.db.commit()

        self.in_memory = True
        logging.info( "Successfully moved database in to memory" )
        self.lock.release()

    def work_from_file( self, overwrite_original = False ):
        if not self.in_memory or self.mem_only:
            logging.error( "Database is already in a file or is restricted to memory only mode!" )
            return

        self.lock.acquire()
        logging.info( "Moving database from memory to a file" )
        with tempfile.TemporaryFile( mode = "w+" ) as tmp:
            for line in self.db.iterdump():
                tmp.write( "{}\n".format( line ) )
            self.db.close()
            tmp.seek(0)

            self.db = sqlite3.connect( self.database_dir + ".mem" )
            self.db.cursor().executescript( tmp.read() )
            self.db.commit()

        if overwrite_original:
            self.db.close()
            os.remove( self.database_dir )
            os.rename( self.database_dir + ".mem", self.database_dir )
            self._load_database()

        self.in_memory = False
        logging.info( "Successfully moved database from memory back to a file" )
        self.lock.release()

    def commit_game( self, game ):
        if not self._valid_game( game ):
            logging.warning( "An invalid game was submitted to the database!\n{}\n".format( game ) )
            return False

        logging.info( "Committing a game to the database" )

        self.lock.acquire()
        try:
            cursor = self.db.cursor()

            match_query = "INSERT OR REPLACE INTO match_info VALUES ( :match_id, :match_time, :winner, :duration, :radiant_score, :dire_score, :skill, :region, :salt, :replay, :throw, :loss );"
            cursor.execute( match_query, game )

            match_id = game["match_id"]

            for i in game["dire_picks"]:
                query = "INSERT OR REPLACE INTO hero_picks VALUES ( ?, ?, ? );"
                cursor.execute( query, ( match_id, 0, i ) )

            for i in game["radiant_picks"]:
                query = "INSERT OR REPLACE INTO hero_picks VALUES ( ?, ?, ? );"
                cursor.execute( query, ( match_id, 1, i ) )

            self.db.commit()
        except:
            self.db.rollback()
            logging.error( "A match insert failed. There was an error with the commit, rolling back." )
            raise
        finally:
            self.lock.release()

        logging.info( "Successfully committed a game to the database!" )
        return True

    def get_drafts( self, starting_from = 0, limit = 1, array = False ):
        if type( limit ) != int or type( starting_from ) != int:
            logging.error( "starting_from or limit were not integers! ({}, {})".format( starting_from, limit ) )
            raise ValueError

        limit = max( 1, limit )
        starting_from = max( 0, starting_from )

        data = None
        limit = 10 * limit          # since we get 10 results per match (10 heroes)
        max_id = 0

        self.lock.acquire()
        try:
            cursor = self.db.cursor()

            query = "SELECT match_info.match_id, match_info.winner, hero_picks.hero, hero_picks.team FROM match_info INNER JOIN hero_picks ON match_info.match_id = hero_picks.match_id WHERE match_info.match_id >= ? LIMIT ?"
            cursor.execute( query, ( starting_from, limit ) )

            data = cursor.fetchall()
        except:
            logging.error( "A draft query failed. There was an error with the commit." )
            raise
        finally:
            self.lock.release()

        if data is not None:
            matches = defaultdict( dict )
            for i in data:
                match_id, winner, hero, team = i
                match = matches[str(match_id)]

                if "win_picks" not in match and "loss_picks" not in match:
                    match["win_picks"] = []
                    match["loss_picks"] = []

                if team == winner:
                    match["win_picks"].append( hero )
                else:
                    match["loss_picks"].append( hero )

                if match_id > max_id:
                    max_id = match_id

            data = matches

        if array:
            data = [ v for _, v in data.items() ]

        num_results = len( data )

        return ( max_id, num_results, data )

    def get_total_examples( self ):
        self.lock.acquire()
        try:
            cursor = self.db.cursor()

            query = "SELECT COUNT(*) FROM match_info"
            cursor.execute( query )

            data = cursor.fetchall()
        except:
            logging.error( "A draft query failed. There was an error with the commit." )
            raise
        finally:
            self.lock.release()

        count = int( data[0][0] )
        return count

    def raw_query( self, query ):
        data = None
        self.lock.acquire()

        try:
            cursor = self.db.cursor()

            cursor.execute( query )
            data = cursor.fetchall()

            self.db.commit()
        except:
            self.db.rollback()
            logging.error( "A raw query failed. There was an error with the commit." )
            raise
        finally:
            self.lock.release()

        return data

    def reset_generator( self ):
        self.match_id_start = 0

