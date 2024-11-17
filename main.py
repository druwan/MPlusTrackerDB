from clean_lua import convert_lua_to_json
from db import create_db, db_exists, load_data_to_db

if __name__ == "__main__":
    try:
        if not db_exists():
            create_db()
            convert_lua_to_json("MPlusTracker.lua", "MPlusTracker.json")
            load_data_to_db("MPlusTracker.json")
        else:
            print("Updating DB")
            convert_lua_to_json("MPlusTracker.lua", "MPlusTracker.json")
            load_data_to_db("MPlusTracker.json")

    except Exception as e:
        print(f"Error from main.py: {e}")
