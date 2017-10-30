from dota2api import API, Database

import asyncio
import logging
import signal
import sys
import time

STATUS_LEVEL = 35

class log_message_count( object ):
    def __init__( self, method ):
        self.method = method
        self.counter = 0

    def __call__( self, *args, **kwargs ):
        self.counter += 1
        return self.method( *args, **kwargs )

def status_log( message, *args, **kwargs ):
    logging.log( STATUS_LEVEL, message, *args, **kwargs )

def init_logging():
    logging.basicConfig( filename = "scraper.log", filemode = "a", format = "%(asctime)s : %(levelname)s : %(message)s", level = logging.WARNING )
    logging.error = log_message_count( logging.error )
    logging.warning = log_message_count( logging.warning )

    logging.addLevelName( STATUS_LEVEL, "STATUS" )
    setattr( logging, "STATUS", STATUS_LEVEL )
    setattr( logging, "status", status_log )

def exit_gracefully( sig, frame ):
    loop.stop()

    logging.status( "--- Caught {}, Exiting ---".format( signal.Signals(sig).name ) )
    sys.exit(0)

if __name__ == "__main__":
    init_logging()
    logging.status( "--- Starting API Poller ---" )

    key = "413B7794E4797A6A070B473F904CA120"
    loop = asyncio.get_event_loop()

    api = API( key = key )
    future = loop.run_in_executor( None, api.run )

    signal.signal( signal.SIGINT, exit_gracefully )
    signal.signal( signal.SIGTERM, exit_gracefully )

    num_matches = 0
    start = time.time()
    with Database( "database" ) as db:
        while True:
            game = api.get_match()
            if not db.commit_game( game ):
                continue

            logging.info( "Found a valid game, committed to the database" )
            num_matches += 1

            error_count = logging.error.counter
            warning_count = logging.warning.counter

            if num_matches % 100 == 0:
                t_since_start = time.time() - start
                logging.status( "There have been {} errors and {} warnings since start ({} non-messages) at a rate of {}s/{}s or {}/{} per successful request".format( error_count, warning_count, num_matches, round( error_count / t_since_start, 3 ), round( warning_count / t_since_start, 3 ), round( error_count / num_matches, 3 ), round( error_count / num_matches, 3 ) ) )

