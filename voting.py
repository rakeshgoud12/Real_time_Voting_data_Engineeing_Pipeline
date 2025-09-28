#!/usr/bin/env python3

import time
import random
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# If you exported delivery_report in main.py you can import it; otherwise we define a tiny one:
try:
    from main import delivery_report
except Exception:
    def delivery_report(err, msg):
        if err:
            print("Delivery failed:", err)
        else:
            try:
                print(
                    f"Delivered to {msg.topic()} [{msg.partition()}] (offset {msg.offset()})")
            except Exception:
                print("Delivered (no metadata)")

# ---------- Config ----------
KAFKA_BOOTSTRAP = "localhost:9092"
VOTERS_TOPIC = "voters_topic"
VOTES_TOPIC = "votes_topic"

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP
}

# ---------- Setup ----------
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)


def fetch_candidates_from_db(cur):
    """
    Returns a list of candidate dicts loaded from the candidates table.
    Each candidate is a Python dict.
    """
    cur.execute("SELECT * FROM candidates;")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    candidates = []
    for row in rows:
        candidate_dict = dict(zip(columns, row))
        candidates.append(candidate_dict)
    return candidates


def insert_vote(conn, cur, vote):
    """
    Insert vote into votes table safely. Commits on success.
    Returns True on success, False on failure.
    """
    try:
        cur.execute(
            """
            INSERT INTO votes (voter_id, candidate_id, voting_time, vote)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (voter_id, candidate_id) DO NOTHING
            """,
            (vote['voter_id'], vote['candidate_id'],
             vote['voting_time'], vote.get('vote', 1))
        )
        conn.commit()
        return True
    except Exception as e:
        # rollback to clear aborted transaction and continue processing
        try:
            conn.rollback()
        except Exception:
            pass
        print("DB insert error (rolled back):", e)
        return False


def run_voting_loop(conn, cur):
    # Load candidates once at start
    candidates = fetch_candidates_from_db(cur)
    if not candidates:
        raise RuntimeError("No candidates found in the database. Aborting.")

    print("Candidates loaded:", len(candidates))
    for candidate in candidates:
        print(
            f"  - {candidate.get('candidate_name', 'Unknown')} ({candidate.get('party_affiliation', 'Unknown')})")

    # Subscribe to voters topic
    consumer.subscribe([VOTERS_TOPIC])
    produced = 0

    try:
        while True:
            try:
                msg = consumer.poll(timeout=1.0)
            except KafkaException as e:
                print("Kafka poll error:", e)
                continue

            if msg is None:
                # no message in this poll window, loop again
                continue

            if msg.error():
                # handle special non-fatal partition EOF signal
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # reached current end of partition; continue polling
                    continue
                else:
                    # fatal-ish error, print and break
                    print("Kafka message error:", msg.error())
                    break

            # valid message: decode JSON and process
            try:
                voter = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print("Failed to decode message value:", e)
                continue

            # choose a random candidate and construct enriched vote record
            chosen = random.choice(candidates)

            # Create simple vote for database
            simple_vote = {
                'voter_id': voter.get('voter_id'),
                'candidate_id': chosen.get('candidate_id'),
                'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                'vote': 1
            }

            # Create enriched vote for Kafka (matching spark-streaming.py schema)
            enriched_vote = {
                'voter_id': voter.get('voter_id'),
                'candidate_id': chosen.get('candidate_id'),
                'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                'voter_name': voter.get('voter_name'),
                'party_affiliation': chosen.get('party_affiliation'),
                'biography': chosen.get('biography'),
                'campaign_platform': chosen.get('campaign_platform'),
                'photo_url': chosen.get('photo_url'),
                'candidate_name': chosen.get('candidate_name'),
                'date_of_birth': voter.get('date_of_birth'),
                'gender': voter.get('gender'),
                'nationality': voter.get('nationality'),
                'registration_number': voter.get('registration_number', ''),
                'address': {
                    'street': voter.get('address_street'),
                    'city': voter.get('address_city'),
                    'state': voter.get('address_state'),
                    'country': voter.get('address_country'),
                    'postalcode': voter.get('address_postcode')
                },
                'email': voter.get('email'),
                'phone_number': voter.get('phone_number'),
                'cell_number': voter.get('cell_number'),
                'picture': voter.get('picture'),
                'registered_age': voter.get('registered_age'),
                'vote': 1
            }

            # Insert simple vote into DB
            ok = insert_vote(conn, cur, simple_vote)
            if not ok:
                # skip producing if DB insert failed
                continue

            # Produce enriched vote to votes_topic
            try:
                producer.produce(
                    VOTES_TOPIC,
                    key=enriched_vote['voter_id'],
                    value=json.dumps(enriched_vote),
                    callback=delivery_report
                )
                produced += 1

                if produced % 100 == 0:
                    print(f"Produced {produced} votes so far...")
                    producer.flush(1)  # Flush periodically

            except Exception as e:
                print("Producer error:", e)

            # commit offset manually after successful processing of this message
            try:
                consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                print("Offset commit failed:", e)

            # small sleep to avoid hammering local services
            time.sleep(0.05)

    finally:
        # cleanup
        try:
            producer.flush(10)
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        print(f"Exiting voting loop, produced {produced} votes.")


if __name__ == "__main__":
    # Connect to Postgres
    conn = psycopg2.connect(
        "host=localhost port=15432 dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    try:
        run_voting_loop(conn, cur)
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
