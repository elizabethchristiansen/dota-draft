This repo contains two components, the first pulls Dota 2 games from various APIs for use by the second component, a neural net that learns how to best complete the draft given a set of heroes by learning on the winning draft from each game.

**Match Pulling**  

The first script is used to pull all ranked all pick games starting now and pulling new games (i.e. is does not go back in history but rather keeps up to date with games being played) via the Dota 2 API to get a list of games and the Dota OpenAPI (YASP) to get details on each individual game (better rate limit). Stores games in a local SQLite3 database.   

To use, create a Python 3.6 virtual environment with

    python3 -m venv <myenvname>

and install the required pip modules with

    pip install -r requirements.txt

You'll also need to install **Keras** and **TensorFlow** which are not included in the _requirements.txt_ file (thanks pip). Additionally, set the `PYTHONPATH` environment variable to include the **bin/** directory.   

To use the script, source the virtual environment and execute the script:

    source <myenvname>
    python data/main.py

Which will begin the script, outputting warning/errors to **scraper.log** in **bin/**. You will need to get a Valve API key to use the script. Once you have it, place it in a _key_ file in the data folder, _data/key_ and **main.py** will read it.

**Draft Completion**  

This part is still a work in progress but the current implementation works (although the predictions may not be perfect). This part uses the winning draft of five heroes from each match, corrupts it and feeds it through a denoising autoencoder with the aim of reconstructing the corrupted heroes. Corruption in this sense means randomly removing between 1 and 4 heroes from the draft (leaving a corrupted draft of 1-4 heroes).

Point the script to your database (generated with the previous script) and run

    python net/net.py

to generate a denoising autoencoder net, _dae.h5_. Currently the script prompts you for some heroes which can be given in the format 

    Hero 1, Hero 2, Hero 3, ...

With any number of heroes (even more than five, it'll simply find the hero that best fits with the heroes you give it). Hero names can be found in the _data/heroes.json_ file.

The script currently does not take in to account the enemy draft (counter drafting) or proper draft composition (supports, ranged/melee balance, etc) and is instead left up to the user, although I do want to implement this in the future.
