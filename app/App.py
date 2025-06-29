import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import streamlit as st
import requests
from google.transit import gtfs_realtime_pb2
import datetime
from zoneinfo import ZoneInfo
import sqlite3

def init_databases():
    # Database used for realtime updates on data, will be wiped clean with every refresh
    conn_realtime = sqlite3.connect('data/realtime.db')
    c_realtime = conn_realtime.cursor()
    c_realtime.execute('''
        CREATE TABLE IF NOT EXISTS trip_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, route_id TEXT, trip_id TEXT, 
            direction_id INTEGER, track_direction TEXT, start_time TEXT,
            start_date TEXT, stop_name TEXT, arrival_time TEXT, 
            departure_time TEXT
        )
    ''')
    conn_realtime.commit()
    conn_realtime.close()

    #Database to store historical data, used for predictive model
    conn_historical = sqlite3.connect('data/historical_data.db')
    c_historical = conn_historical.cursor()
    c_historical.execute('''
        CREATE TABLE IF NOT EXISTS trip_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, route_id TEXT, trip_id TEXT, 
            direction_id INTEGER, track_direction TEXT, start_time TEXT,
            start_date TEXT, stop_name TEXT, arrival_time TEXT, 
            departure_time TEXT, UNIQUE(trip_id, start_date, stop_name)
        )
    ''')
    conn_historical.commit()
    conn_historical.close()

def fetch_mta_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        return None

def process_and_store_data(responses):
    stops_df = pd.read_csv("data/stops.txt", index_col=0)
    
    conn_realtime = sqlite3.connect('data/realtime.db')
    conn_historical = sqlite3.connect('data/historical_data.db')
    c_realtime = conn_realtime.cursor()
    c_historical = conn_historical.cursor()

    c_realtime.execute('DELETE FROM trip_updates')

    for response in responses:
        if not response:
            continue
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response)
        except Exception as e:
            print(f"Error parsing a feed: {e}")
            continue

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip = entity.trip_update.trip
                route_id = trip.route_id
                trip_id = trip.trip_id
                direction_id = trip.direction_id
                start_time = trip.start_time
                start_date = trip.start_date

                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id

                    if stop_id.endswith('N'):
                        track_direction = 'Northbound'
                    elif stop_id.endswith('S'):
                        track_direction = 'Southbound'
                    else:
                        track_direction = 'Unknown'
                    
                    arrival_time = stop_time_update.arrival.time
                    arrival_dt = datetime.datetime.fromtimestamp(arrival_time, tz=ZoneInfo("America/New_York"))
                    arrival_dt = str(arrival_dt)
                    departure_time = stop_time_update.departure.time
                    departure_dt = datetime.datetime.fromtimestamp(departure_time, tz=ZoneInfo("America/New_York"))
                    departure_dt = str(departure_dt)
                    try:
                        stop_name = stops_df.loc[stop_id, "stop_name"]
                    except:
                        stop_name = stop_id
                    
                    data_tuple = (route_id, trip_id, direction_id, track_direction, start_time, start_date, 
                                    stop_name, arrival_dt[11:19], departure_dt[11:19])
                    c_realtime.execute('''
                        INSERT INTO trip_updates (route_id, trip_id, direction_id, track_direction, start_time, start_date, 
                            stop_name, arrival_time, departure_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', data_tuple)
                    
                    c_historical.execute('''
                        INSERT OR IGNORE INTO trip_updates (route_id, trip_id, direction_id, track_direction, start_time, start_date, 
                            stop_name, arrival_time, departure_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', data_tuple)

    conn_realtime.commit()
    conn_realtime.close()
    conn_historical.commit()
    conn_historical.close()
    return 

def get_data_from_db():
    conn = sqlite3.connect('data/realtime.db')
    df = pd.read_sql_query('SELECT * FROM trip_updates', conn)
    conn.close()
    return df

# --- Streamlit App ---

st.title('MTA Subway Real-Time Tracker')

MTA_FEEDS = {
    '1, 2, 3, 4, 5, 6, 7, S': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    'A, C, E, H': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    'N, Q, R, W': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    'B, D, F, M': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    'L': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    'G': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    'J, Z': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    'SIR': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}

if 'last_update' not in st.session_state:
    st.session_state.last_update = None

init_databases()

col1, col2 = st.columns([3, 1])

with col1:
    if st.button('Refresh Live Data'):
        with st.spinner ('Fetching latest data from MTA...'):
            all_responses = []
            for feed_name, feed_url in MTA_FEEDS.items():
                st.write(f"Fetching {feed_name}...")
                response_content = fetch_mta_data(feed_url)
                all_responses.append(response_content)
            
            process_and_store_data(all_responses)
            st.session_state.last_update = datetime.datetime.now(ZoneInfo('America/New_York'))
            st.success('Data refreshed!')

with col2:
    if st.session_state.last_update:
        st.caption(f"Last updated: {st.session_state.last_update.strftime('%I:%M:%S %p')} EST")

st.header('Current Trip Information')
try:
    df = get_data_from_db()

    if not df.empty:
        route_list = sorted(df['route_id'].unique())

        selected_route = st.selectbox('Select a Subway Line:', route_list)

        filtered_df = df[df['route_id'] == selected_route]

        northbound_df = filtered_df[filtered_df['track_direction'] == 'Northbound'].sort_values(by='arrival_time')
        southbound_df = filtered_df[filtered_df['track_direction'] == 'Southbound'].sort_values(by='arrival_time')
        
        st.subheader('Northbound')
        if not northbound_df.empty:
            for trip_id, trip_df in northbound_df.groupby('trip_id'):
                with st.expander(f"Train ID: {trip_id}"):
                    display_trip = trip_df[['stop_name', 'arrival_time']].rename(columns={'stop_name': 'Station', 'arrival_time': 'Est. Arrival'})
                    st.dataframe(display_trip.reset_index(drop=True))
        else:
            st.write('No Northbound trains found.')

        st.subheader('Southbound')
        if not southbound_df.empty:
            for trip_id, trip_df in southbound_df.groupby('trip_id'):
                with st.expander(f"Train ID: {trip_id}"):
                    display_trip = trip_df[['stop_name', 'arrival_time']].rename(columns={'stop_name': 'Station', 'arrival_time': 'Est. Arrival'})
                    st.dataframe(display_trip.reset_index(drop=True))
        else:
            st.write('No southbound trains found.')

    else:
        st.info('No data in the database. Click "Refresh Live Data" to begin.')

except Exception as e:
    st.error(f"Could not load data from database. Error: {e}")
