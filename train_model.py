import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

data = pd.read_csv('training_data.csv')

def scheduled_arrival_seconds(training_df):
    time_str_column = training_df['scheduled_arrival']
    arrival_seconds = time_str_column.apply(lambda x: (int(x[0:2]) * 3600) + (int(x[3:5]) * 60) + int(x[6:8]))
    training_df['scheduled_arrival_seconds'] = arrival_seconds

def day_of_the_week(training_df):
    date_str_column = training_df['start_date'].astype(str)
    date_literal = pd.to_datetime(date_str_column, format='%Y%m%d')
    day_int = date_literal.dt.dayofweek
    day = day_int.apply(get_service_day)
    training_df['service_day'] = day

def get_service_day(day_int):
    if day_int == 5:
        return 'Saturday'
    elif day_int == 6:
        return 'Sunday'
    else:
        return 'Weekday'

def data_preparation(training_df):
    categorical_features = ['route_id', 'track_direction', 'stop_id', 'service_day']
    numerical_features = ['scheduled_arrival_seconds']
    target = 'delay_seconds'

    training_df_encoded = pd.get_dummies(training_df, columns=categorical_features)

    features = numerical_features + [col for col in training_df_encoded.columns if any(col.startswith(cat + '_') for cat in categorical_features)]
    X = training_df_encoded[features]
    y = training_df_encoded[target]

    return X, y

def main():
    day_of_the_week(data)
    scheduled_arrival_seconds(data)
    X, y = data_preparation(data)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(random_state=42)

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    error = mean_absolute_error(y_test, y_pred)
    r_squared = r2_score(y_test, y_pred)

    print(f"Mean Absolute Error: {error:.2f} seconds")
    print(f"R-squared: {r_squared:.4f}")

    joblib.dump(model, 'mta_delay_model.joblib')

    print("Model saved successfully!")

if __name__ == '__main__':
    main()