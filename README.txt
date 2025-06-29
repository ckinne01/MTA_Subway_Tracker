The purpose of this app currently is to track the subway in NYC in real time,
offering predictions on delays based off of machine learning models in 
Python. Currently the app can differentiate between lines and shows distinct
trains and where they are currently headed. The data is being stored in a 
SQLite database that is just a file within my app as there is not very much 
data being processed at this time. The next goal is to build a predictive
model for the stops using stored data. Eventually our dataset will be too big
to store locally so we'll have to find a new hosting service. 

In order to run this app locally, simply install the entire project folder, 
navigate to the project folder in your terminal, and type the command 
"streamlit run app/App.py".
Now you will be able to see the app on your local network and use all of it's
functinality. Eventually I want to make this a publically available site but
for now it can only be run locally