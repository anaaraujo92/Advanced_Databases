import pandas as pd
from pymongo import MongoClient
import time
import pprint

client2 = MongoClient()
Client2 = MongoClient("localhost", 27017)

# convert do dataframe and read csv files
const_stands_df2 = pd.read_csv("constructor_standings.csv")
const_df2 = pd.read_csv("constructors.csv")
race_df2 = pd.read_csv("races.csv")

print(const_stands_df2.head())
print(const_stands_df2.shape)

print(const_df2.head())
print(const_df2.shape)

print(race_df2.head())
print(race_df2.shape)

data_cs2 = const_stands_df2.to_dict(orient="records")
data_c2 = const_df2.to_dict(orient="records")
data_r2 = race_df2.to_dict(orient="records")

# create database formula1
db2 = client2.formula1

db2.constructor_standings2.drop()
db2.constructors2.drop()
db2.races2.drop()

# create collection for standings
constructor_standing_2 = db2.constructor_standings2
constructor_standing2_2 = constructor_standing_2.insert_many(data_cs2)

# create index for standings collection
db2.constructor_standings2.create_index("constructorStandingsId", unique=True)

# remove data that is not relevant
db2.constructor_standings2.update_many({}, {"$unset": {"points": ""}})
db2.constructor_standings2.update_many({}, {"$unset": {"position": ""}})
db2.constructor_standings2.update_many({}, {"$unset": {"positionText": ""}})

# remove "_id" in standings
db2.constructor_standings2.find({}, {'_id': False})

# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
const_st2 = constructor_standing_2.find()
list_const_st2 = list(const_st2)
df_const_st2 = pd.DataFrame(list_const_st2)
print(df_const_st2.head())

# create collection for constructors
constructor_2 = db2.constructors2
constructor2_2 = constructor_2.insert_many(data_c2)
# create index for constructors collection
db2.constructors2.create_index("constructorId", unique=True)
# remove data that is not relevant
db2.constructors2.update_many({}, {"$unset": {"constructorRef": ""}})
db2.constructors2.update_many({}, {"$unset": {"url": ""}})
# remove "_id" in constructors
db2.constructors2.find({}, {'_id': False})
# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
const2 = constructor_2.find()
list_const2 = list(const2)
df_const2 = pd.DataFrame(list_const2)
print(df_const2.head())

# create collection for races
racing_2 = db2.races2
racing2_2 = racing_2.insert_many(data_r2)
# create index for races collection
db2.races2.create_index("raceId", unique=True)
# remove data that is not relevant
db2.races2.update_many({}, {"$unset": {"round": ""}})
db2.races2.update_many({}, {"$unset": {"circuitId": ""}})
db2.races2.update_many({}, {"$unset": {"name": ""}})
db2.races2.update_many({}, {"$unset": {"date": ""}})
db2.races2.update_many({}, {"$unset": {"time": ""}})
db2.races2.update_many({}, {"$unset": {"url": ""}})
db2.races2.update_many({}, {"$unset": {"fp1_date": ""}})
db2.races2.update_many({}, {"$unset": {"fp1_time": ""}})
db2.races2.update_many({}, {"$unset": {"fp2_date": ""}})
db2.races2.update_many({}, {"$unset": {"fp2_time": ""}})
db2.races2.update_many({}, {"$unset": {"fp3_date": ""}})
db2.races2.update_many({}, {"$unset": {"fp3_time": ""}})
db2.races2.update_many({}, {"$unset": {"quali_date": ""}})
db2.races2.update_many({}, {"$unset": {"quali_time": ""}})
db2.races2.update_many({}, {"$unset": {"sprint_date": ""}})
db2.races2.update_many({}, {"$unset": {"sprint_time": ""}})
# remove "_id" in races
db2.races2.find({}, {'_id': False})

# to verify data was added to collection, it was converted to list, than to data frame and print the first 5 rows.
races1_2 = racing_2.find()
list_race2 = list(races1_2)
df_races2 = pd.DataFrame(list_race2)
print(df_races2.head())

# joins the three tables, besides the optimization with indexes, it was also sorted by wins and year and limited to 10
results2 = db2.constructor_standings2.aggregate([{
    "$lookup":
        {
        "from": "constructors2",  # collection 2: constructors
        "localField": "constructorId",  # key field in collection 2
        "foreignField": "constructorId",  # key field in collection 1
        "as": "teams2"  # alias for resulting table
    }}, {"$unwind": "$teams2"},
    {"$lookup": {
        "from": "races2",  # collection 3: results
        "localField": "raceId",  # key field in collection 3
        "foreignField": "raceId",  # key field in collection 1
        "as": "races2"  # alias for resulting table
    }}, {"$unwind": "$races2"},
    {"$sort": {"wins": -1, "year": -1}},
    {"$limit": 10},
    {"$project": {"_id": 0, 'constructorId': 1, 'wins':"", 'teams2.name': 1, 'teams2.nationality': 1,
                  'races2.raceId': 1,
                  'races2.year': 1}}])

for x in results2:
    pprint.pprint(x)



# function for the  testing of queries:
def performance(collection, query):
    time_i = time.time()
    F1_nosql_with_index = collection.find(query)
    time_f = time.time()
    print('time expended ', time_f - time_i)


# query1 testing performance
performance(constructor_2, { "$and" : [{"name": {"$regex": "^M" } }, { "name": {"$regex":"s$" }}]})


