import requests
import time
import os
import logging
import sys
import threading
import queue


class ReplayDownloader( threading.Thread ):
    def __init__( self, queue, replay_dir = "" ):
        super( ReplayDownloader, self ).__init__()

        self.queue = queue
        self.dir = replay_dir
        self.rate = 10
        self.rate_additional = 30
        self.rate_additional_base = self.rate_additional
        self.exit = False
        logging.info( "Initialized replay downloader" )

    def run( self ):
        logging.info( "Starting replay downloader thread" )
        while True:
            if self.exit:
                logging.status( "Exited the replay downloader thread!" )
                break

            try:

                try:
                    match_id, url = self.queue.get( timeout = 15 )
                except queue.Empty:
                    continue

                tries = 5
                while tries > 0:
                    logging.info( "Getting replay {}".format( url ) )
                    r = requests.get( url )
                    if r.status_code == 200:
                        name = str( match_id ) + ".dem.bz2"
                        path = os.path.abspath( self.dir + "replays/" + name )
                        with open( path, "wb" ) as rep:
                            rep.write( r.content )

                        logging.info( "Wrote {}!".format( name ) )
                    elif r.status_code == 404:
                        logging.warning( "Replay could not be found! [{}, status code: {}]".format( r.url, r.status_code ) )
                    else:
                        logging.warning( "Replay pull had a non-200 status code! [{}, status code: {}]".format( r.url, r.status_code ) )
                        time.sleep( self.rate_additional )
                        self.rate_additional += self.rate
                        tries -= 1
                        continue

                    self.rate_additional = max( self.rate_additional - self.rate, self.rate_additional_base )
                    break
                else:
                    logging.error( "Could not get replay data after 5 tries! [{}, status code: {}]".format( r.url, r.status_code) )

                self.queue.task_done()
                time.sleep( self.rate )

            except BaseException as e:
                logging.exception( "We encountered a fatal error ({}) in the replay downloader. Sleeping for a long time and trying again.".format( str( e ) ) )
                time.sleep( 1800 )

    def quit( self ):
        self.exit = True
