import json

from lupa import LuaRuntime


def clean_name(name):
    return name.rstrip("*").strip()


# Define the recursive conversion function
def lua_table_to_dict(lua_table):
    if lua_table is None:
        return None
    if isinstance(lua_table, (int, float, str, bool)):
        return lua_table

    # Check for integer keys to detect array-like tables
    max_int_key = max(lua_table.keys(), default=0)
    is_array = all(
        isinstance(key, int) and 1 <= key <= max_int_key for key in lua_table.keys()
    )

    # Handle as list if array-like
    if is_array:
        return [lua_table_to_dict(lua_table[i]) for i in range(1, max_int_key + 1)]
    else:
        # Handle as dictionary if not array-like, skipping unwanted keys
        return {
            str(k): lua_table_to_dict(v)
            for k, v in lua_table.items()
            if k not in ["started", "incomplete", "completed"]
        }


def convert_lua_to_json(lua_file_path, json_output_path):
    """
    Converts a Lua table from a given Lua file to JSON, excluding specific keys.

    Args:
        lua_file_path (str): Path to the Lua file containing the Lua table.
        json_output_path (str): Path to save the output JSON file.
    """
    # Initialize Lua runtime
    lua = LuaRuntime(unpack_returned_tuples=True)

    # Load Lua file content
    with open(lua_file_path, "r", encoding="utf-8") as lua_file:
        lua_content = lua_file.read()

    # Execute the Lua script to get the table in Lua's global environment
    lua.execute("MPT_DB = nil")  # Reset any previous value for safety
    lua.execute(lua_content)  # Load MPT_DB from the file

    # Access the MPT_DB table in Lua
    mpt_db = lua.globals().MPT_DB

    # Convert MPT_DB from Lua to a Python dictionary
    mpt_db_dict = lua_table_to_dict(mpt_db)

    # Clean names
    for run in mpt_db_dict.get("runs", []):
        for party_member in run.get("party", []):
            party_member["name"] = clean_name(party_member["name"])

    # Save the converted data to JSON format
    with open(json_output_path, "w", encoding="utf-8") as json_file:
        json.dump(mpt_db_dict, json_file, indent=2, ensure_ascii=False)

    print(f"Conversion completed. Data saved as {json_output_path}.")


# Example usage:
# convert_lua_to_json("MPlusTracker.lua", "MPlusTracker.json")
