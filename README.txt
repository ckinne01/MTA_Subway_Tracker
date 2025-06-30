The purpose of this app currently is to track the subway in NYC in real time,
offering predictions on delays based off of machine learning models in 
Python. Currently the app can differentiate between lines and shows distinct
trains and where they are currently headed. The data is being stored in a 
SQLite database that is just a file within my app as there is not very much 
data being processed at this time. The next goal is to build a predictive
model for the stops using stored data. So far I have a script built to
create a training set that has the overall delays from the historical_data.db
database. The next step is to use that training data to build the predictive
model. 

In order to run this app locally, simply install the entire project folder, 
navigate to the project folder in your terminal, and type the command 
"streamlit run app/App.py".
Now you will be able to see the app on your local network and use all of it's
functinality. Eventually I want to make this a publically available site but
for now it can only be run locally.

To build your own training set you simply have to navigate into the 
MTA_SUBWAY_TRACKER project folder and then run the script 
build_training_data.py. This will generate a training file that you can use for
the prediction file that I will be making soon.