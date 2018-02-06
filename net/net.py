from dota2api import Database

import tensorflow as tf

from keras import backend as K
from keras.backend import tensorflow_backend as Ktf
from keras.models import Model, load_model
from keras.layers import Input, Dense

from math import floor
from random import random, shuffle
from json import load, dump
from collections import defaultdict

import logging
import numpy as np
import sys


np.set_printoptions(threshold=np.inf)


class DraftAutoEncoder():
    def __init__( self, *, data = None, hero_json = None, encoding_dim = 100, batch_size = 10, epochs = 1, denoise = False, validation_perc = 0.0 ):
        logging.info( "Starting autoencoder class" )

        self.data = data
        self.hero_json = hero_json
        self.encoding_dim = encoding_dim
        self.batch_size = batch_size
        self.epochs = epochs
        self.denoise = denoise
        self.validation_perc = validation_perc
        self.data_total = self.data.get_total_examples()

        self.train_data_total = floor( self.data_total * ( 1.0 - self.validation_perc ) )
        self.validation_data_total = self.data_total - self.train_data_total

        self.max_train_id = self.data.get_percentile_id( ( 1.0 - validation_perc ) )
        self.validation_id_start = self.max_train_id + 1
        self.train_id_start = 0

        self.input_size = 115

        self.pos_weight = 12

        self._create_mappings()
        self._compute_class_weights()

    def _create_mappings( self ):
        self.id_to_name = {}
        self.name_to_id = {}
        self.id_to_raw_id = {}
        self.raw_id_to_id = {}

        for k, v in self.hero_json.items():
            hero_id = int( k )
            hero_raw_id = int( v["raw_id"] )
            hero_name = v["name"]

            self.id_to_name[k] = hero_name
            self.name_to_id[hero_name] = hero_id
            self.id_to_raw_id[k] = hero_raw_id
            self.raw_id_to_id[str( hero_raw_id )] = hero_id

    def _compute_class_weights( self ):
        try:
            with open( "class_weights.json", "r" ) as cw:
                self.class_weights = load( cw )
            self.class_weights = { int( k ): v for k, v in self.class_weights.items() }
        except OSError:
            self.class_weights = np.zeros( self.input_size )

            num_data = 0
            id_start = 0
            limit = 1000
            while True:
                max_id, num_results, data = self.data.get_drafts( starting_from = id_start, limit = limit, array = True )
                for i in data:
                    for hero in i["win_picks"]:
                        self.class_weights[self.id_to_raw_id[str( hero )]] += 1

                id_start = max_id + 1
                num_data += num_results
                if num_data >= self.data_total:
                    break

                if num_data % 10000 == 0:
                    sys.stdout.write( "\rCounting class occurrences, {0}% completed.".format( round( 100 * num_data / self.data_total, 2 ) ) )
                    sys.stdout.flush()

            average_num = np.average( self.class_weights )
            self.class_weights = ( 1.0 / ( self.class_weights / average_num ) ) ** 2
            self.class_weights = { n: round( i, 3 ) for n, i in enumerate( self.class_weights ) }

            with open( "class_weights.json", "w" ) as cw:
                dump( self.class_weights, cw )

    def _drop_heroes( self, data, rate = 0.5 ):
        if self.denoise:
            draft = list( data )
            shuffle( draft )

            for _ in range( 5 ):
                if random() <= rate:
                    draft.pop()

                if len( draft ) <= 1:
                    break

            return draft
        else:
            return data

    def _batch_data( self, data ):
        batch_x = []
        batch_y = []

        for n, match in enumerate( data ):
            win_picks = [ self.id_to_raw_id[str( i )] for i in match["win_picks"] ]
            loss_picks = [ self.id_to_raw_id[str( i )] for i in match["loss_picks"] ]
            dropped_win_picks = self._drop_heroes( win_picks, rate = 0.6 )

            draft = np.zeros( self.input_size )
            draft[win_picks] = 1

            draft_drop = np.zeros( self.input_size )
            draft_drop[dropped_win_picks] = 1

            batch_x.append( draft_drop )
            batch_y.append( draft )

        return ( [ np.array( batch_x ) ], [ np.array( batch_y ) ] )

    def _validation_generator( self ):
        while True:
            max_id, num_results, data = self.data.get_drafts( starting_from = self.validation_id_start, limit = self.batch_size, array = True )

            if num_results < self.batch_size:
                self.validation_id_start = self.max_train_id + 1
                continue
            else:
                self.validation_id_start = max_id + 1

            yield self._batch_data( data )

    def _train_generator( self ):
        while True:
            max_id, num_results, data = self.data.get_drafts( starting_from = self.train_id_start, limit = self.batch_size, array = True )

            if num_results < self.batch_size or max_id > self.max_train_id:
                self.train_id_start = 0
                continue
            else:
                self.train_id_start = max_id + 1

            yield self._batch_data( data )

    def _build_model( self, input_ ):
        # deep_1 = Dense( self.encoding_dim * 3, activation = "elu" )( input_ )
        # deep_2 = Dense( self.encoding_dim * 2, activation = "elu" )( deep_1 )
        encoding = Dense( self.encoding_dim, activation = "elu" )( input_ )
        # deep_3 = Dense( self.encoding_dim * 2, activation = "elu" )( encoding )
        # deep_4 = Dense( self.encoding_dim * 3, activation = "elu" )( deep_3 )
        output = Dense( self.input_size, activation = "sigmoid" )( encoding )

        return output

    def _weighted_binary_crossentropy( self, target, output ):
        _epsilon = Ktf._to_tensor( Ktf.epsilon(), output.dtype.base_dtype )
        output = tf.clip_by_value( output, _epsilon, 1 - _epsilon )
        output = tf.log( output / ( 1 - output ) )

        loss = tf.nn.weighted_cross_entropy_with_logits( targets = target, logits = output, pos_weight = self.pos_weight )
        return tf.reduce_mean( loss, axis = -1 )

    def train_or_load( self, model_dir = "dae.h5" ):
        try:
            self.load( load_dir = model_dir )
        except OSError:
            self.train( save_dir = model_dir )

    def load( self, load_dir = "dae.h5" ):
        logging.info( "Trying to load the net from file" )
        self.net = load_model( load_dir, custom_objects = { "_weighted_binary_crossentropy": self._weighted_binary_crossentropy } )

    def train( self, save_dir = "dae.h5" ):
        logging.info( "Building new net from scratch" )
        if self.data is None:
            logging.error( "An attempt to build the autoencoder was made without supplying data to train from!" )
            exit()      # turn this in to a proper error

        input_ = Input( shape = ( self.input_size, ) )
        output = self._build_model( input_ )

        self.net = Model( input_, output )
        self.net.compile( optimizer = "adam", loss = self._weighted_binary_crossentropy, metrics = [ "binary_accuracy", "mean_absolute_error" ] )

        train_steps_per_epoch = floor( self.train_data_total / self.batch_size )
        validation_steps_per_epoch = floor( self.validation_data_total / self.batch_size )

        fit_args = { "generator": self._train_generator(), "steps_per_epoch": train_steps_per_epoch, "epochs": self.epochs, "class_weight": self.class_weights }
        if self.validation_perc > 0.0:
            fit_args["validation_data"] = self._validation_generator()
            fit_args["validation_steps"] = validation_steps_per_epoch

        self.net.fit_generator( **fit_args )

        self.net.save( save_dir )

    def _names_to_vector( self, names ):
        draft = np.zeros( self.input_size )

        for i in names:
            hero_id = self.name_to_id[i]
            hero_raw_id = self.id_to_raw_id[str( hero_id )]

            draft[hero_raw_id] = 1

        return np.reshape( draft, ( 1, self.input_size ) )

    def _vector_to_names( self, vector ):
        names = []

        for i in vector:
            hero_id = self.raw_id_to_id[str( i )]
            hero_name = self.id_to_name[str( hero_id )]
            names.append( hero_name )

        return names

    def complete_draft( self, heroes, k = 5 ):
        heroes_f = self._names_to_vector( heroes )
        prediction = self.net.predict( heroes_f )[0]
        prediction_sorted = np.argsort( prediction )
        top_five = prediction_sorted[::-1][:k]

        names = self._vector_to_names( top_five )
        percentages = [ "{}%".format( round( i * 100, 2 ) ) for i in prediction[top_five] ]
        return zip( names, percentages )

if __name__ == "__main__":
    # set up logging in __main__ - can be handled by other scripts if the module is imported
    def status_log( message, *args, **kwargs ):
        logging.log( 35, message, *args, **kwargs )

    logging.basicConfig( format = "%(asctime)s : %(levelname)s : %(message)s", level = logging.INFO )

    logging.addLevelName( 35, "STATUS" )
    setattr( logging, "STATUS", 35 )
    setattr( logging, "status", status_log )

    with open( "../data/heroes.json", "r" ) as heroes:
        heros_json = load( heroes )["heroes"]

    with Database( "database", check_same_thread = False ) as db:
        dae = DraftAutoEncoder( data = db, hero_json = heros_json, encoding_dim = 20, batch_size = 30, epochs = 1, denoise = True, validation_perc = 0.2 )
        dae.train_or_load()

    print( "Test some drafts!" )
    while True:
        incomplete_draft = input( "Draft: " ).split( ", " )
        print( list( dae.complete_draft( incomplete_draft, k = 10 ) ) )
