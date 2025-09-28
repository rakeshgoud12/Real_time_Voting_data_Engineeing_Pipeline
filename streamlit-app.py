# real_time_election_dashboard.py
import time
import uuid
import pandas as pd
import simplejson as json
import streamlit as st
from confluent_kafka import Consumer
from streamlit_autorefresh import st_autorefresh
import psycopg2
import matplotlib.pyplot as plt

# ---------------------------
# Configurations for Kafka and Postgres
# ---------------------------
KAFKA_BOOTSTRAP = "localhost:9092"  # Your Kafka bootstrap server
AGG_VOTES_TOPIC = "aggregated_votes_per_candidate"
AGG_TURN_TOPIC = "aggregated_turnout_by_location"
POSTGRES_CONN_STR = "host=localhost port=15432 dbname=voting user=postgres password=postgres"

# Candidate party affiliations (A-Z by candidate name)
CANDIDATE_PARTY_MAPPING = {
    "Dylan Obrien": "America_Party",
    "Flenn Bates": "Democratic_Party",
    "Kristen Harrison": "Republic_Party"
}

# Replace/override unprofessional photos for specific candidates with local/professional images.
# Update this mapping with candidate_name -> professional image path or URL.
PROFESSIONAL_PHOTO_OVERRIDE = {
    # Professional photos in suits (A-Z by candidate name)
    # Professional headshot in suit
    "Dylan Obrien": "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?w=400&h=400&fit=crop&crop=face",
    # Professional headshot in suit - 50-55 year old man
    "Flenn Bates": "https://images.unsplash.com/photo-1560250097-0b93528c311a?w=400&h=400&fit=crop&crop=face",
    # Professional headshot in suit - 50-55 year old woman
    "Kristen Harrison": "https://i0.wp.com/www.jtouchofstyle.com/wp-content/uploads/2018/05/psDSC_0831-853x1280.jpg?resize=682%2C1024&ssl=1",
    # Alternative local file options (uncomment and place your image files):
    # "Dylan Obrien": "dylan_obrien_suit.jpg",  # Local professional photo
    # "Flenn Bates": "flenn_bates_suit.jpg",  # Local professional photo
    # "Kristen Harrison": "kristen_harrison_suit.jpg",  # Local professional photo
}

# ---------------------------
# Utilities
# ---------------------------


def create_kafka_consumer(topic_name: str) -> Consumer:
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': f'streamlit-group-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest',  # Start reading from earliest offset each time
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    return consumer


@st.cache_data(show_spinner=False)
def fetch_voting_stats():
    try:
        conn = psycopg2.connect(POSTGRES_CONN_STR)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM voters;")
        voters_count = cur.fetchone()[0] or 0
        cur.execute("SELECT count(*) FROM candidates;")
        candidates_count = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
        return voters_count, candidates_count
    except Exception as e:
        # Do not crash dashboard for DB errors — show 0,0 and a message
        st.error(f"Error reading Postgres stats: {e}")
        return 0, 0


def fetch_data_from_kafka(consumer: Consumer, max_messages=10000):
    """
    Fetch available messages from the Kafka topic from the beginning.
    Each message should decode to JSON.
    """
    data = []
    try:
        while len(data) < max_messages:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                # optionally log error: msg.error()
                continue
            value = msg.value()
            if value is None:
                continue
            try:
                decoded = json.loads(value.decode("utf-8"))
                data.append(decoded)
            except Exception:
                # skip decode errors
                continue
    finally:
        try:
            consumer.close()
        except Exception:
            pass
    return data


def plot_bar_chart_from_dict(data_dict: dict, title="Distribution", max_states=10):
    # Create a cleaner bar chart instead of cluttered pie chart
    labels = list(data_dict.keys())
    sizes = list(data_dict.values())

    # Sort by values and take top states for better visibility
    sorted_data = sorted(zip(labels, sizes), key=lambda x: x[1], reverse=True)
    top_labels, top_sizes = zip(*sorted_data[:max_states])

    fig, ax = plt.subplots(figsize=(12, 6))
    if top_sizes and sum(top_sizes) > 0:
        bars = ax.bar(range(len(top_labels)), top_sizes,
                      color='skyblue', edgecolor='navy', alpha=0.7)
        ax.set_xticks(range(len(top_labels)))
        ax.set_xticklabels(top_labels, rotation=45, ha='right')
        ax.set_ylabel('Total Votes')
        ax.set_title(f"{title} (Top {max_states} States)")

        # Add value labels on bars
        for i, (bar, value) in enumerate(zip(bars, top_sizes)):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(top_sizes)*0.01,
                    f'{int(value)}', ha='center', va='bottom', fontsize=9)
    else:
        ax.text(0.5, 0.5, "No data", ha='center',
                va='center', transform=ax.transAxes)
        ax.set_title(title)

    plt.tight_layout()
    return fig


def choose_best_photo(candidate_name: str, row: pd.Series):
    """
    Preference order:
    1) professional_photo_url (field in Kafka message)
    2) mapping override (local professional image specified in script)
    3) photo_url (fallback)
    4) None
    """
    # 1) check explicit professional_photo_url in message
    professional = None
    if "professional_photo_url" in row and pd.notna(row["professional_photo_url"]) and isinstance(row["professional_photo_url"], str) and row["professional_photo_url"].strip():
        professional = row["professional_photo_url"].strip()

    # 2) mapping override by candidate name (local file or trusted URL)
    if not professional and candidate_name in PROFESSIONAL_PHOTO_OVERRIDE:
        professional = PROFESSIONAL_PHOTO_OVERRIDE[candidate_name]

    # 3) fallback to generic photo_url field (but we might consider it "unprofessional")
    if not professional and "photo_url" in row and pd.notna(row["photo_url"]) and isinstance(row["photo_url"], str) and row["photo_url"].strip():
        professional = row["photo_url"].strip()

    # 4) else None
    return professional


# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="Real-time Election Dashboard", layout="wide")
st.title("Real-time Election Dashboard")

# Autorefresh
_ = st_autorefresh(interval=10 * 1000, key="auto_refresh")

with st.sidebar:
    st.header("Controls")
    refresh_interval = st.slider(
        "Auto-refresh interval (seconds)", 5, 60, 10, 5)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")
    page_size = st.selectbox("Table page size", [5, 10, 25, 50], index=1)
    page_number = st.number_input("Page", 1, value=1, step=1)

voters_count, candidates_count = fetch_voting_stats()
col1, col2 = st.columns(2)
col1.metric("Total Voters", voters_count)
col2.metric("Total Candidates", candidates_count)
st.markdown("---")

# ---------------------------
# Aggregated votes per candidate (live)
# ---------------------------
st.header("Aggregated Votes per Candidate (live)")

votes_consumer = create_kafka_consumer(AGG_VOTES_TOPIC)
raw_votes = fetch_data_from_kafka(votes_consumer)

# Expect messages like:
# {"candidate_name":"Flenn Bates","total_votes":338,"photo_url":"http://...","professional_photo_url":null,"party":"Independent"}
votes_df = pd.json_normalize(raw_votes) if raw_votes else pd.DataFrame()

# Ensure required columns exist and normalize types
for col in ["candidate_name", "total_votes", "photo_url", "professional_photo_url", "party"]:
    if col not in votes_df.columns:
        votes_df[col] = None

votes_df["candidate_name"] = votes_df["candidate_name"].fillna("N/A")
votes_df["total_votes"] = pd.to_numeric(
    votes_df["total_votes"], errors="coerce").fillna(0).astype(int)
votes_df["party"] = votes_df["party"].fillna(
    "Independent")  # default party if missing

# Aggregate by candidate_name and professional/photo (we prefer grouping by name only)
votes_agg = (
    votes_df.groupby(["candidate_name", "party"], as_index=False)
    .agg({"total_votes": "sum"})
    # Sort A-Z by candidate name
    .sort_values("candidate_name", ascending=True)
)

if not votes_agg.empty:
    st.subheader("Candidates and Their Vote Counts")

    # Create a cleaner grid layout for candidates
    for i, (_, row) in enumerate(votes_agg.iterrows()):
        candidate_name = row["candidate_name"]
        total_votes = int(row["total_votes"])
        # Use party from mapping if available, otherwise fall back to data
        party = CANDIDATE_PARTY_MAPPING.get(candidate_name, row["party"])

        # Skip Independent/N/A candidates
        if candidate_name == "N/A" or candidate_name == "Larry Ed":
            continue

        # Create a card-like container for each candidate
        with st.container():
            col1, col2 = st.columns([2, 4])

            with col1:
                # Find a representative row for this candidate to retrieve photos
                candidate_rows = votes_df[votes_df["candidate_name"]
                                          == row["candidate_name"]]
                chosen_photo = None
                if not candidate_rows.empty:
                    chosen_photo = choose_best_photo(
                        candidate_name, candidate_rows.iloc[0])

                # Display image
                if chosen_photo:
                    try:
                        st.image(chosen_photo, width=150,
                                 caption=None, use_container_width=False)
                    except Exception as e:
                        # Try alternative image loading method
                        try:
                            st.image(chosen_photo, width=150,
                                     use_container_width=False)
                        except:
                            st.write("📷 Image loading...")
                            st.write(f"**{candidate_name}**")
                else:
                    st.write("📷 No image available")
                    st.write(f"**{candidate_name}**")

            with col2:
                # Display candidate information
                st.markdown(f"## {candidate_name}")
                st.markdown(f"**Party:** {party}")
                st.markdown(f"**Votes:** {total_votes:,}")

                # Add some visual separation
                if i < len(votes_agg) - 1:
                    st.markdown("---")

else:
    st.info("No candidates with votes yet.")

st.markdown("---")

# ---------------------------
# Turnout by location (live)
# ---------------------------
st.header("Turnout by Location (live)")

loc_consumer = create_kafka_consumer(AGG_TURN_TOPIC)
raw_loc = fetch_data_from_kafka(loc_consumer)
loc_df = pd.json_normalize(raw_loc) if raw_loc else pd.DataFrame()

if not loc_df.empty and "state" in loc_df.columns and "total_votes" in loc_df.columns:
    loc_df["total_votes"] = pd.to_numeric(
        loc_df["total_votes"], errors="coerce").fillna(0).astype(int)
    loc_grouped = loc_df.groupby("state", as_index=False)["total_votes"].sum()
    st.dataframe(loc_grouped.sort_values(
        "total_votes", ascending=False), use_container_width=True)

    # Plot bar chart (cleaner than pie chart for many states)
    turnout_dict = dict(zip(loc_grouped["state"], loc_grouped["total_votes"]))
    bar_chart = plot_bar_chart_from_dict(
        turnout_dict, title="Turnout by State", max_states=15)
    st.pyplot(bar_chart)
elif loc_df.empty:
    st.info("No location turnout data available in Kafka yet.")
else:
    st.info("No valid state/turnout data in Kafka topic.")

st.markdown("---")
st.write(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
