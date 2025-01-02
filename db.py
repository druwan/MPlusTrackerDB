import json
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from psycopg import connect, sql
from psycopg.errors import UniqueViolation

load_dotenv()


def db_exists():
    """
    Checks if the specified database exists.
    Returns True if it exists, False otherwise.
    """
    try:
        with connect(
            dbname="postgres",
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWD"),
            host=os.getenv("DBHOST"),
            port=os.getenv("DBPORT"),
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s;",
                    (os.getenv("DBNAME"),),
                )
                return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking db: {e}")
        return False


def create_db():
    """
    Creates the specified db
    """
    new_db = os.getenv("DBNAME")
    try:
        with connect(
            dbname="postgres",
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWD"),
            host=os.getenv("DBHOST"),
            port=os.getenv("DBPORT"),
        ) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {};").format(sql.Identifier(new_db))
                )
                print("db created")
        for _ in range(5):
            try:
                # Try to connect to the new database
                with connect(
                    dbname=new_db,
                    user=os.getenv("DBUSER"),
                    password=os.getenv("DBPASSWD"),
                    host=os.getenv("DBHOST"),
                    port=os.getenv("DBPORT"),
                ) as conn:
                    print(f"Successfully connected to the {new_db} database.")
                    return  # Exit if the connection is successful
            except Exception as e:
                print(f"Error {e} connecting to {new_db} to be ready... Retrying...")
                time.sleep(2)  # Wait for 2 seconds before retrying
        raise Exception(
            f"Failed to connect to the {new_db} database after multiple attempts."
        )
    except Exception as e:
        print(f"Error creating db: {e}")


def ensure_unique_constraint(conn):
    """
    Ensure the unique constraint on the 'runs' table.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'runs'::regclass 
                    AND contype = 'u' 
                    AND conname = 'unique_start_time';
            """
        )
        if cursor.fetchone() is None:
            # Add constraint
            cursor.execute(
                """
                    ALTER TABLE runs
                    ADD CONSTRAINT unique_start_time UNIQUE (start_time);
                """
            )
            print("Unique constraint added to 'start_time' col.")


def load_data_to_db(json_file_path):
    """Load data from JSON into the db"""
    with open(json_file_path, "r") as f:
        data = json.load(f)

    # Connect to db
    with connect(
        dbname=os.getenv("DBNAME"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWD"),
        host=os.getenv("DBHOST"),
        port=os.getenv("DBPORT"),
    ) as conn:
        # PostgreSQL tables
        create_db_tables = [
            """
            CREATE TABLE IF NOT EXISTS runs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                completion_time INT,
                affix_names TEXT,
                level INT,
                map_name TEXT,
                start_time TIMESTAMP,
                completion_timestamp TIMESTAMP
            );
            """,
            """
                CREATE TABLE IF NOT EXISTS party (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id UUID REFERENCES runs(id),
                role TEXT,
                name TEXT,
                class TEXT,
                spec TEXT 
            );
            """,
        ]

        with conn.cursor() as cursor:  # Create tables
            for query in create_db_tables:
                cursor.execute(query)
            conn.commit()

        # Ensure constraint
        ensure_unique_constraint(conn)

        # Insert data into the 'runs' table
        run_ids = []
        with conn.cursor() as cursor:
            for run in data["runs"]:
                try:
                    start_time = datetime.strptime(
                        run["startTime"], "%Y-%m-%d %H:%M:%S"
                    )
                    completion_time = run["completionTime"] / 1000
                    completion_timestamp = start_time + timedelta(
                        seconds=completion_time
                    )

                    cursor.execute(
                        """
                        INSERT INTO runs (completion_time, affix_names, level, map_name, start_time, completion_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time) DO UPDATE
                        SET completion_time = EXCLUDED.completion_time,
                            affix_names = EXCLUDED.affix_names,
                            level = EXCLUDED.level,
                            map_name = EXCLUDED.map_name
                        RETURNING id;
                        """,
                        (
                            run["completionTime"],
                            run["affixNames"],
                            run["level"],
                            run["mapName"],
                            start_time,
                            completion_timestamp,
                        ),
                    )
                    run_ids.append(cursor.fetchone()[0])
                except UniqueViolation:
                    print(f"Duplicate start_time detected for {run['startTime']}")
            conn.commit()

        # Insert data into 'party' table
        party_records = []
        for run_id, run in zip(run_ids, data["runs"]):
            for party_member in run["party"]:
                party_records.append(
                    (
                        run_id,
                        party_member["role"],
                        party_member["name"],
                        party_member["class"],
                        party_member.get("spec"),
                    )
                )

        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO party (run_id, role, name, class, spec)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                party_records,
            )
            conn.commit()
        print(f"db {os.getenv("DBNAME")} populated")
