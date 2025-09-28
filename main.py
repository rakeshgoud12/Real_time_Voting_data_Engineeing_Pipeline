#!/usr/bin/env python3
"""
Robust main.py — defensive RandomUser fetching and safe DB/Kafka workflow.
- Avoids IndexError when API returns empty results
- Retries API, skips failures
- Batch flush to Kafka (no per-message flush)
- DB commits and rollbacks to avoid aborted transactions
"""

import json
import time
import random
import requests
import psycopg2
from confluent_kafka import Producer

# ----- CONFIG -----
BASE_URL = "https://randomuser.me/api/?nat=us"
PARTIES = ["Republic Party", "Democratic Party", "America Party"]
random.seed(42)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "voters_topic"
POSTGRES_CONN = "host=localhost port=15432 dbname=voting user=postgres password=postgres"

NUM_VOTERS = 1000
BATCH_COMMIT = 100        # how often to flush/commit
API_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 0.01

# ----- HELPERS -----


def safe_get_results(resp):
    """Return first result dict or None if missing/empty."""
    try:
        data = resp.json()
    except Exception:
        return None
    results = data.get("results") if isinstance(data, dict) else None
    if not results:
        return None
    return results[0]


# ----- GENERATORS -----
def generate_voter_data(retries=API_RETRIES, timeout=5):
    """
    Robustly fetch one voter. Returns dict or None after retries.
    Handles network errors, non-200, missing results.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, timeout=timeout)
        except Exception:
            # network error, back off and retry
            time.sleep(0.3)
            continue

        if resp.status_code != 200:
            time.sleep(0.3)
            continue

        user = safe_get_results(resp)
        if not user:
            # API returned empty results, retry
            time.sleep(0.3)
            continue

        # build flattened voter dict matching DB columns
        return {
            "voter_id": user["login"]["uuid"],
            "voter_name": f"{user['name']['first']} {user['name']['last']}",
            "date_of_birth": user["dob"]["date"],
            "gender": user["gender"],
            "nationality": user.get("nat", ""),
            "address_street": f"{user['location']['street']['number']} {user['location']['street']['name']}",
            "address_city": user["location"]["city"],
            "address_state": user["location"]["state"],
            "address_country": user["location"]["country"],
            "address_postcode": str(user["location"]["postcode"]),
            "email": user["email"],
            "phone_number": user["phone"],
            "cell_number": user.get("cell", ""),
            "picture": user["picture"]["large"],
            "registered_age": user["registered"]["age"],
        }
    return None


# ----- DB / TABLE CREATION -----
def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255),
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote INT DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()


# ----- DB INSERT (safe) -----
def insert_voter(conn, cur, voter):
    try:
        cur.execute("""
            INSERT INTO voters (
                voter_id, voter_name, date_of_birth, gender, nationality,
                address_street, address_city, address_state,
                address_country, address_postcode, email, phone_number,
                cell_number, picture, registered_age
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (voter_id) DO NOTHING
        """, (
            voter["voter_id"], voter["voter_name"], voter["date_of_birth"],
            voter["gender"], voter["nationality"], voter["address_street"], voter["address_city"],
            voter["address_state"], voter["address_country"], voter["address_postcode"],
            voter["email"], voter["phone_number"], voter["cell_number"],
            voter["picture"], voter["registered_age"]
        ))
        # commit immediately for safety in dev environment
        conn.commit()
        print("Inserted voter:", voter)   # you asked to see full dict
        return True
    except Exception as e:
        # clear aborted transaction state and continue
        try:
            conn.rollback()
        except Exception:
            pass
        print("DB insert error (rolled back):", e)
        return False


# ----- Kafka delivery callback -----
def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        try:
            print(
                f"Message delivered to {msg.topic()} [{msg.partition()}] (offset {msg.offset()})")
        except Exception:
            print("Message delivered (no metadata available)")


# ----- MAIN -----
def main():
    # Connect Kafka & Postgres
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    conn = psycopg2.connect(POSTGRES_CONN)
    cur = conn.cursor()

    try:
        create_tables(conn, cur)

        # ensure we have candidates (keeps your existing code's behavior)
        cur.execute("SELECT COUNT(*) FROM candidates")
        cnt = cur.fetchone()[0]
        if cnt == 0:
            # insert 3 candidates
            for i in range(3):
                candidate = None
                # try to fetch candidate defensively
                try:
                    resp = requests.get(
                        BASE_URL + f"&gender={'female' if i%2==1 else 'male'}", timeout=5)
                    candidate_user = safe_get_results(resp)
                    if candidate_user:
                        candidate = {
                            "candidate_id": candidate_user["login"]["uuid"],
                            "candidate_name": f"{candidate_user['name']['first']} {candidate_user['name']['last']}",
                            "party_affiliation": PARTIES[i % len(PARTIES)],
                            "biography": "A brief biography of the candidate",
                            "campaign_platform": "Key campaign promises and/or platform",
                            "photo_url": candidate_user["picture"]["large"]
                        }
                except Exception:
                    pass
                if candidate:
                    cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (candidate_id) DO NOTHING
                    """, (
                        candidate["candidate_id"], candidate["candidate_name"], candidate["party_affiliation"],
                        candidate["biography"], candidate["campaign_platform"], candidate["photo_url"]
                    ))
            conn.commit()
            print("Inserted initial candidates.")

        produced = 0
        # produce voters
        for i in range(NUM_VOTERS):
            voter = generate_voter_data()
            if voter is None:
                # failed to fetch after retries; skip
                print(
                    f"Warning: failed to fetch voter at iteration {i}. Skipping.")
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                continue

            ok = insert_voter(conn, cur, voter)
            if not ok:
                # insertion failed (and rolled back) — skip producing
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                continue

            # produce to Kafka
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=voter["voter_id"],
                    value=json.dumps(voter),
                    callback=delivery_report
                )
                produced += 1
            except Exception as e:
                print("Producer error:", e)

            # periodic flush (not every message)
            if i % BATCH_COMMIT == 0 and i > 0:
                producer.flush(5)

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        # final flush & commit
        producer.flush(10)
        print(f"Done: produced {produced} voters and inserted into DB.")

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
