import json
import os
import uuid

import polars as pl
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values

load_dotenv()


def db_exists():
    """
    Checks if the specified database exists.
    Returns True if it exists, False otherwise.
    """
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWD"),
            host=os.getenv("DBHOST"),
            port=os.getenv("DBPORT"),
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s;",
                (os.getenv("DBNAME"),),
            )
            if cursor.fetchone():
                return True
            else:
                print(f"DB {os.getenv("DBNAME")} does not exist, creating...")
                return False

    except psycopg2.Error as e:
        print(f"Error checking db: {e}")
    finally:
        if conn:
            conn.close()


def create_db():
    """
    Creates the specified db
    """
    new_db = os.getenv("DBNAME")
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASSWD"),
            host=os.getenv("DBHOST"),
            port=os.getenv("DBPORT"),
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {};").format(sql.Identifier(new_db))
            )
            print("db created")

    except psycopg2.Error as e:
        print(f"Error creating db: {e}")
    finally:
        if conn:
            conn.close()


def load_data_to_db(json_file_path):
    """ """
    # Load data
    with open(json_file_path, "r") as f:
        data = json.load(f)

    # Load data into Polars DF
    runs_data = pl.DataFrame(data["runs"])

    # Connect to db
    conn = psycopg2.connect(
        dbname=os.getenv("DBNAME"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWD"),
        host=os.getenv("DBHOST"),
        port=os.getenv("DBPORT"),
    )

    cursor = conn.cursor()

    # PostgreSQL tables
    create_db_tables = [
        """
        CREATE TABLE IF NOT EXISTS runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        completion_time INT,
        affix_names TEXT,
        level INT,
        map_name TEXT,
        start_time TIMESTAMP
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

    # Create tables
    for query in create_db_tables:
        cursor.execute(query)
    conn.commit()

    # Insert data into the 'runs' table
    runs_data = [
        (
            run["completionTime"],
            run["affixNames"],
            run["level"],
            run["mapName"],
            run["startTime"],
        )
        for run in data["runs"]
    ]

    run_ids = [str(uuid.uuid4()) for _ in range(len(runs_data))]

    execute_values(
        cursor,
        """
        INSERT INTO runs (id, completion_time, affix_names, level, map_name, start_time)
        VALUES %s;
        """,
        [(run_id, *run) for run_id, run in zip(run_ids, runs_data)],
    )

    # Prepare party data
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

    execute_values(
        cursor,
        """
    INSERT INTO party (run_id, role, name, class, spec)
    VALUES %s;
    """,
        party_records,
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"db {os.getenv("DBNAME")} populated")
