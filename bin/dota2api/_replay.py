import requests
import os
import logging
import sys
import asyncio
import time


class ReplayDownloader( object ):
    def __init__( self, replay_dir = "" ):
        self.queue = asyncio.Queue()
        self.dir = replay_dir
        self.rate = 10
        self.rate_additional = 30
        self.rate_additional_base = self.rate_additional
        self.events = asyncio.get_event_loop()
        self.heartbeat = 0
        logging.info( "Initialized replay downloader" )

    async def _process( self ):
        while True:
            try:
                if ( time.time() - self.heartbeat ) >= 3600:
                    logging.status( "[Replay Downloader] I'm still alive! Queue has {} items.".format( self.queue.qsize() ) )
                    self.heartbeat = time.time()

                try:
                    match_id, url = await asyncio.wait_for( self.queue.get(), 600 )
                except asyncio.TimeoutError:
                    logging.warning( "The replay downloader queue has been empty for {} seconds [Downloader can't pull]!".format( 600 ) )
                    continue

                tries = 5
                while tries > 0:
                    logging.info( "Getting replay {}".format( url ) )
                    future_res = self.events.run_in_executor( None, requests.get, url )
                    r = await future_res
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
                        await asyncio.sleep( self.rate_additional )
                        self.rate_additional += self.rate
                        tries -= 1
                        continue

                    self.rate_additional = max( self.rate_additional - self.rate, self.rate_additional_base )
                    break
                else:
                    logging.error( "Could not get replay data after 5 tries! [{}, status code: {}]".format( r.url, r.status_code) )

                self.queue.task_done()
                await asyncio.sleep( self.rate )

            except BaseException as e:
                logging.exception( "We encountered a fatal error ({}) in the replay downloader. Sleeping for a long time and trying again.".format( str( e ) ) )
                await asyncio.sleep( 1800 )
                logging.status( "Waking the replay downloader after a fatal error sleep" )

    def add_game( self, game ):
        self.queue.put_nowait( game )

    def run( self ):
        logging.info( "Initializing replay downloader event loop" )
        self.events.create_task( self._process() )
        self.events.run_forever()
