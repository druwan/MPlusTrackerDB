import os
import re
import sys
from slpp import slpp as lua
from datetime import datetime, timedelta
import psycopg


DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = "192.168.1.104"
DB_PORT = "5432"
MPT_PATH_DEFAULT = "./data/MPT.lua"
MPT_PATH = os.getenv("MPT_LUA_PATH") or MPT_PATH_DEFAULT
if len(sys.argv) > 1:
    MPT_PATH = sys.argv[1]


def connect_to_db(db_name, user, password, host, port):
    """Connect to PSQL"""
    try:
        conn = psycopg.connect(
            dbname=db_name, user=user, password=password, host=host, port=port
        )
        print(f"Connected to db: '{db_name}'")
        return conn
    except psycopg.Error as e:
        print(f"Error connecting to db {db_name}: {e}")
        exit(1)


def create_tables(cursor):
    """Create runs and party table"""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS runs (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          char TEXT NOT NULL,
          season INT,
          completion_time INT,
          affix_names TEXT[],
          key_level INT NOT NULL,
          map_name VARCHAR(100) NOT NULL,
          start_time TIMESTAMP NOT NULL,
          completion_timestamp TIMESTAMP NOT NULL,
          completed BOOLEAN NOT NULL DEFAULT FALSE,
          on_time BOOLEAN,
          keystone_upgrade_levels INT DEFAULT 0,
          old_overall_dungeon_score INT DEFAULT 0,
          new_overall_dungeon_score INT DEFAULT 0,
          num_deaths INT DEFAULT 0,
          time_lost INT DEFAULT 0,
          CONSTRAINT unique_run UNIQUE(char, start_time)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS party (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          run_id UUID REFERENCES runs(id) ON DELETE CASCADE,
          role VARCHAR(255),
          name VARCHAR(255),
          class VARCHAR(255),
          spec VARCHAR(255)
        );
    """)
    print("Tables ensured")


def parse_mpt_lua(mpt_path):
    """Parse the MPT.lua file and return data structure"""
    if not os.path.exists(mpt_path):
        print(f"Error: MPT.lua not found at {mpt_path}")
        exit(1)
    with open(mpt_path, "r") as f:
        lua_text = f.read()
        lua_text = re.sub(
            r"^(MPT_DB|MPT_DB_GLOBAL)\s*=\s*", "", lua_text, flags=re.MULTILINE
        )
        data = lua.decode("{" + lua_text + "}")
    mpt_db = data[0]
    mpt_db_global = data[1] if len(data) > 1 else {"totalRuns": 0, "totalCompleted": 0}
    return mpt_db, mpt_db_global


def export_run(cursor, run):
    party = run.get("party", run.get("group", []))
    char = run.get("char")
    if char in (None, "") and isinstance(party, list):
        for member in party:
            name = member.get("name", "")
            if name.endswith("*"):
                char = name.rstrip("*")
                break
    if not char:
        char = "Unknown"

    season = run.get("season", 1) if run.get("season") is not None else 1
    affixes = run.get("affixes", [run["affixNames"]] if "affixNames" in run else [""])
    completion_time = run.get("completionTime")
    start_timestamp = datetime.strptime(run["startTime"], "%Y-%m-%d %H:%M:%S")
    completion_timestamp = run.get("completion_timestamp") or run.get("endTime")
    if not completion_timestamp and completion_time:
        completion_seconds = completion_time / 1000
        completion_timestamp = (
            start_timestamp + timedelta(seconds=completion_seconds)
        ).strftime("%Y-%m-%d %H:%M:%S")

    values = (
        char,
        season,
        completion_time,
        affixes,
        run.get("level", run.get("keyLvl")),
        run["mapName"],
        run["startTime"],
        completion_timestamp,
        run["completed"],
        run.get("onTime"),
        run.get("keystoneUpgradeLevels", 0),
        run.get("oldOverallDungeonScore", 0),
        run.get("newOverallDungeonScore", 0),
        run.get("numDeaths", 0),
        run.get("timeLost", 0),
    )

    cursor.execute(
        """
        INSERT INTO runs (
            char, season, completion_time, affix_names, key_level, map_name, start_time, completion_timestamp,
            completed, on_time, keystone_upgrade_levels, old_overall_dungeon_score, new_overall_dungeon_score,
            num_deaths, time_lost
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT unique_run DO NOTHING
        RETURNING id
    """,
        values,
    )

    result = cursor.fetchone()
    run_id = result[0] if result else None

    if run_id:
        party_records = []
        if isinstance(party, list):
            for member in party:
                party_records.append(
                    (
                        run_id,
                        member["role"],
                        member["name"],
                        member["class"],
                        member.get("spec"),
                    )
                )
        else:
            tank = party.get("tank", {})
            if tank:
                party_records.append(
                    (run_id, "TANK", tank["name"], tank["class"], tank.get("spec"))
                )
            healer = party.get("healer", {})
            if healer:
                party_records.append(
                    (
                        run_id,
                        "HEALER",
                        healer["name"],
                        healer["class"],
                        healer.get("spec"),
                    )
                )
            for dps in party.get("dps", []):
                party_records.append(
                    (run_id, "DAMAGER", dps["name"], dps["class"], dps.get("spec"))
                )

        if party_records:
            cursor.executemany(
                """
                INSERT INTO party (run_id, role, name, class, spec)
                VALUES (%s, %s, %s, %s, %s)
            """,
                party_records,
            )
    return run_id is not None


def main():
    conn = connect_to_db(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    cursor = conn.cursor()
    create_tables(cursor)
    conn.commit()
    mpt_db, mpt_db_global = parse_mpt_lua(MPT_PATH)

    # Any new runs to process?
    if "runs" not in mpt_db or not mpt_db["runs"]:
        print("No runs to insert, resetting MPT.lua and exiting")
        mpt_db["runs"] = []
        if "unsyncedRuns" in mpt_db:
            mpt_db["unsyncedRuns"] = []
        with open(MPT_PATH, "w") as f:
            f.write(
                f"MPT_DB = {lua.encode(mpt_db)}\nMPT_DB_GLOBAL = {lua.encode(mpt_db_global)}\n"
            )
        conn.commit()
        conn.close()
        return

    runs_to_process = mpt_db.get("unsyncedRuns", range(1, len(mpt_db["runs"]) + 1))
    any_new_runs = False
    for run_idx in runs_to_process:
        if export_run(cursor, mpt_db["runs"][run_idx - 1]):
            any_new_runs = True

    if "unsyncedRuns" in mpt_db:
        mpt_db["unsyncedRuns"] = []
    print(
        f"{'New runs inserted' if any_new_runs else 'All runs already synced'}, updating MPT.lua with cleared unsyncedRuns"
    )
    with open(MPT_PATH, "w") as f:
        f.write(
            f"MPT_DB = {lua.encode(mpt_db)}\nMPT_DB_GLOBAL = {lua.encode(mpt_db_global)}\n"
        )
    conn.commit()
    conn.close()
    print("Exported runs to postgresql")


if __name__ == "__main__":
    main()
