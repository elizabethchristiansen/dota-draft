Simple script to pull all ranked all pick games starting now and pulling new games (i.e. is does not go back in history but rather keeps up to date with games being played) via the Dota 2 API to get a list of games and the Dota OpenAPI (YASP) to get details on each individual game (better rate limit). Stores games in a local SQLite3 database.   

To use, create a Python 3.6 (3.5+) virtual environment with

    python3 -m venv <myenvname>

and install the required pip modules with

    pip install -r requirements.txt

You'll also have to set the `PYTHONPATH` environment variable to include the **bin/** directory.   

To use the script, source the virtual environment and execute the script:

    source <myenvname>
    python data/main.py

Which will begin the script, outputting warning/errors to **scraper.log** in **bin/**.    

**TODO:**    
* Use **Keras** to implement a neural net for counter draft prediction