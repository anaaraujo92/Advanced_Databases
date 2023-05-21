import pprint
import time

import pandas as pd
from pymongo import MongoClient

client = MongoClient()

Client = MongoClient("localhost", 27017)

# convert do dataframe and read csv files
const_stands_df = pd.read_csv("constructor_standings.csv")
const_df = pd.read_csv("constructors.csv")
race_df = pd.read_csv("races.csv")

print(const_stands_df.head())
print(const_stands_df.shape)

print(const_df.head())
print(const_df.shape)

print(race_df.head())
print(race_df.shape)

data_cs = const_stands_df.to_dict(orient="records")
data_c = const_df.to_dict(orient="records")
data_r = race_df.to_dict(orient="records")

# create database formula1
db = client.formula1

db.constructor_standings.drop()
db.constructors.drop()
db.races.drop()

# create collection standings
constructor_standing = db.constructor_standings
constructor_standing2 = constructor_standing.insert_many(data_cs)

# remove data not relevant
db.constructor_standings.update_many({}, {"$unset": {"points": ""}})
db.constructor_standings.update_many({}, {"$unset": {"position": ""}})
db.constructor_standings.update_many({}, {"$unset": {"positionText": ""}})
# remove "_id" from standings
db.constructor_standings.find({}, {'_id': False})
# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
const_st = constructor_standing.find()
list_const_st = list(const_st)
df_const_st = pd.DataFrame(list_const_st)
print(df_const_st.head())

# create collection constructors
constructor = db.constructors
constructor2 = constructor.insert_many(data_c)
# remove data not relevant
db.constructors.update_many({}, {"$unset": {"constructorRef": ""}})
db.constructors.update_many({}, {"$unset": {"url": ""}})
db.constructors.find({}, {'_id': False})
# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
const = constructor.find()
list_const = list(const)
df_const = pd.DataFrame(list_const)
print(df_const.head())

# create collection races
racing = db.races
racing2 = racing.insert_many(data_r)
# remove data not relevant
db.races.update_many({}, {"$unset": {"round": ""}})
db.races.update_many({}, {"$unset": {"circuitId": ""}})
db.races.update_many({}, {"$unset": {"name": ""}})
db.races.update_many({}, {"$unset": {"date": ""}})
db.races.update_many({}, {"$unset": {"time": ""}})
db.races.update_many({}, {"$unset": {"url": ""}})
db.races.update_many({}, {"$unset": {"fp1_date": ""}})
db.races.update_many({}, {"$unset": {"fp1_time": ""}})
db.races.update_many({}, {"$unset": {"fp2_date": ""}})
db.races.update_many({}, {"$unset": {"fp2_time": ""}})
db.races.update_many({}, {"$unset": {"fp3_date": ""}})
db.races.update_many({}, {"$unset": {"fp3_time": ""}})
db.races.update_many({}, {"$unset": {"quali_date": ""}})
db.races.update_many({}, {"$unset": {"quali_time": ""}})
db.races.update_many({}, {"$unset": {"sprint_date": ""}})
db.races.update_many({}, {"$unset": {"sprint_time": ""}})
# remove "_id" from standings
db.races.find({}, {'_id': False})
# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
races1 = racing.find()
list_race = list(races1)
df_races = pd.DataFrame(list_race)
print(df_races.head())

# joins the three tables without sort by wins and year and limit to 10
results = db.constructor_standings.aggregate([{"$lookup": {
    "from": "constructors",  # collection 2: constructors
    "localField": "constructorId",  # key field in collection 2
    "foreignField": "constructorId",  # key field in collection 1
    "as": "teams"  # alias for resulting table
}}, {"$unwind": "$teams"},
    {"$lookup": {
        "from": "races",  # collection 3: results
        "localField": "raceId",  # key field in collection 3
        "foreignField": "raceId",  # key field in collection 1
        "as": "races"  # alias for resulting table
    }}, {"$unwind": "$races"},
    {"$project": {"_id": 0, 'constructorId': 1, 'wins': 1, 'teams.name': 1, 'teams.nationality': 1,
                  'races.raceId': 1,
                  'races.year': 1}}])

for i in results:
    pprint.pprint(i)

client.close()

def performance(collection, query):
    time_i = time.time()
    F1_nosql_without_index = collection.find(query)
    time_f = time.time()
    print('time expended ', time_f - time_i)


performance(constructor_standing, { "$and" : [{"name": {"$regex": "^A" } }, { "name": {"$regex":"C$" }}]})


