import pandas as pd
import sqlite3
import datetime

def robust_time_parser(time_str):
    """
    A robust function to parse time strings, handling standard 'HH:MM:SS'
    and MTA's 'HH:MM:SS' where HH can be >= 24.
    Returns a datetime.time object or None.
    """
    if not isinstance(time_str, str):
        return None
    try:
        # Try standard parsing first
        return datetime.datetime.strptime(time_str, '%H:%M:%S').time()
    except ValueError:
        # If standard parsing fails, try MTA's extended hour format
        try:
            parts = time_str.split(':')
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            # Create a timedelta from midnight to handle hours > 23
            return (datetime.datetime.min + datetime.timedelta(hours=h, minutes=m, seconds=s)).time()
        except (ValueError, IndexError):
            return None

def calculate_delay(row):
    """
    Calculates the delay in seconds between a scheduled and actual time,
    handling overnight trips correctly.
    """
    scheduled_time = robust_time_parser(row['scheduled_arrival'])
    actual_time = robust_time_parser(row['actual_arrival'])

    if scheduled_time is None or actual_time is None:
        return None

    dummy_date = datetime.date.today()
    scheduled_dt = datetime.datetime.combine(dummy_date, scheduled_time)
    actual_dt = datetime.datetime.combine(dummy_date, actual_time)

    # Handle overnight case: if scheduled is late night and actual is early morning
    # and the difference is significant (e.g., more than 12 hours apart, implying next day)
    if actual_dt < scheduled_dt and (scheduled_dt - actual_dt).total_seconds() > 12 * 3600:
        actual_dt += datetime.timedelta(days=1)

    return (actual_dt - scheduled_dt).total_seconds()

def get_service_id_for_date(date_str, calendar_df):
    """
    Determines the service_id for a given date string (YYYYMMDD).
    """
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    
    dt_obj = datetime.date(year, month, day)
    day_of_week = dt_obj.weekday() # Monday is 0, Sunday is 6

    day_map = {
        0: 'monday', 1: 'tuesday', 2: 'wednesday', 
        3: 'thursday', 4: 'friday', 5: 'saturday', 6: 'sunday'
    }
    day_col = day_map.get(day_of_week)

    if day_col:
        # Filter calendar_df for active service on this date and day of week
        active_services = calendar_df[
            (calendar_df['start_date'] <= int(date_str)) & 
            (calendar_df['end_date'] >= int(date_str)) &
            (calendar_df[day_col] == 1)
        ]['service_id'].tolist()
        
        # Return the first active service_id found, or None
        return active_services[0] if active_services else None
    return None

def main():
    print("Starting to build the training dataset...")

    # --- 1. Load Data ---
    conn = sqlite3.connect('data/historical_data.db')
    realtime_df = pd.read_sql_query("SELECT * FROM trip_updates", conn)
    conn.close()

    if realtime_df.empty:
        print("The historical_data.db is empty. Please run the main app to collect some data first.")
        return

    try:
        stop_times_df = pd.read_csv('data/stop_times.txt', dtype=str)
        trips_df = pd.read_csv('data/trips.txt', dtype=str)
        calendar_df = pd.read_csv('data/calendar.txt', dtype={'start_date': int, 'end_date': int})
    except FileNotFoundError as e:
        print(f"Error: Could not find a required GTFS file: {e}")
        print("Please ensure 'stop_times.txt', 'trips.txt', and 'calendar.txt' are in the 'data' directory.")
        return

    # --- 2. Prepare and Merge Data ---
    print("Preparing and merging data...")
    
    realtime_df.rename(columns={'arrival_time': 'actual_arrival'}, inplace=True)
    stop_times_df.rename(columns={'arrival_time': 'scheduled_arrival'}, inplace=True)

    # --- Step 2a: Determine Service ID for each real-time record ---
    print("Determining service IDs for real-time data...")
    # Ensure start_date is integer for comparison with calendar_df
    realtime_df['start_date'] = realtime_df['start_date'].astype(int)
    realtime_df['service_id'] = realtime_df['start_date'].apply(lambda x: get_service_id_for_date(str(x), calendar_df))
    
    # Drop rows where service_id could not be determined
    realtime_df.dropna(subset=['service_id'], inplace=True)
    if realtime_df.empty:
        print("No real-time data could be matched to a service ID. Check calendar.txt dates.")
        return

    # --- Step 2b: Match real-time trips to static trips ---
    print("Matching real-time trips to static schedule trips...")
    
    # The real-time trip_id often contains the static trip_id as a substring.
    # We need to find the static trip_id that corresponds to the real-time trip.
    # This is the most complex part and might need fine-tuning based on MTA's exact GTFS.
    
    # Let's try to match based on route_id, direction_id, and a derived start_time from trip_id
    # This is a common pattern for MTA GTFS.
    
    # Extract start_time from real-time trip_id (e.g., '000000_FS.S01R' -> '000000')
    realtime_df['realtime_start_time_str'] = realtime_df['trip_id'].apply(lambda x: x.split('_')[0] if '_' in x else x)
    
    # Extract start_time from static trip_id (e.g., 'AFA23GEN-1038-Sunday-00_000600_1..S03R' -> '000600')
    trips_df['static_start_time_str'] = trips_df['trip_id'].apply(lambda x: x.split('_', 1)[-1].split('_')[0] if '_' in x else x)
    
    # Merge real-time data with trips_df to get the static trip_id
    # We need to match on route_id, direction_id, service_id, and derived start_time
    
    # Ensure data types are consistent for merging
    realtime_df['route_id'] = realtime_df['route_id'].astype(str)
    trips_df['route_id'] = trips_df['route_id'].astype(str)
    realtime_df['direction_id'] = realtime_df['direction_id'].astype(str) # Convert to str for consistent merge
    trips_df['direction_id'] = trips_df['direction_id'].astype(str)
    
    # Perform the merge to link real-time records to static trip_ids
    # This merge is crucial for getting the correct static trip_id
    matched_trips_df = pd.merge(
        realtime_df,
        trips_df[['trip_id', 'route_id', 'direction_id', 'service_id', 'static_start_time_str']],
        left_on=['route_id', 'direction_id', 'service_id', 'realtime_start_time_str'],
        right_on=['route_id', 'direction_id', 'service_id', 'static_start_time_str'],
        how='inner',
        suffixes=('_realtime', '_static')
    )
    
    if matched_trips_df.empty:
        print("No real-time trips could be matched to static trips. Check trip_id parsing and merge keys.")
        return

    # --- Step 2c: Final Merge with stop_times.txt ---
    print("Merging with stop_times.txt to get scheduled arrivals...")
    
    # Ensure stop_id is string for merging
    matched_trips_df['stop_id'] = matched_trips_df['stop_id'].astype(str)
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)

    # Now merge on the actual static trip_id and stop_id
    merged_df = pd.merge(
        matched_trips_df,
        stop_times_df[['trip_id', 'stop_id', 'scheduled_arrival']],
        left_on=['trip_id_static', 'stop_id'], # Use the static trip_id from the previous merge
        right_on=['trip_id', 'stop_id'],
        how='inner',
        suffixes=('_merged', '_stop_times')
    )

    # --- 3. Calculate Delay & 4. Save ---
    if merged_df.empty:
        print("Final merge resulted in an empty DataFrame. No delays to calculate.")
        return
        
    print(f"Final merge successful! Found {len(merged_df)} records for delay calculation.")
    print("Calculating delays...")
    merged_df['delay_seconds'] = merged_df.apply(calculate_delay, axis=1)

    output_path = 'training_data.csv'
    print(f"Saving final dataset to {output_path}...")
    
    final_columns = [
        'route_id', 'trip_id_realtime', 'track_direction', 'stop_id', 'stop_name', 
        'start_date', 'start_time', 'scheduled_arrival', 'actual_arrival', 'delay_seconds'
    ]
    final_df = merged_df[[col for col in final_columns if col in merged_df.columns]].copy()
    
    # Drop rows where delay could not be calculated (None values)
    print(f"Found {len(final_df['delay_seconds'].dropna())} rows with a calculated delay before filtering.")
    final_df.dropna(subset=['delay_seconds'], inplace=True)
    
    # Filter out nonsensical delays (e.g., more than 2 hours in either direction)
    initial_rows = len(final_df)
    final_df = final_df[final_df['delay_seconds'].abs() < 7200]
    print(f"Filtered out {initial_rows - len(final_df)} rows with extreme delays.")
    
    final_df.to_csv(output_path, index=False)
    
    print(f"Done! Training data saved to {output_path}")
    print(f"Successfully created a dataset with {len(final_df)} rows.")

if __name__ == '__main__':
    main()
