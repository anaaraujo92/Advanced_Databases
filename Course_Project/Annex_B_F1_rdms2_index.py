import csv
import sqlite3
import time
import pandas as pd

# create a database named formula1
df_const = pd.read_csv('constructors.csv')
df_stand = pd.read_csv('constructor_standings.csv')
df_results = pd.read_csv('races.csv')
conn = sqlite3.connect('results2.db')

c2 = conn.cursor()

#create table constructors
c2.execute("""CREATE TABLE IF NOT EXISTS constructors2 (
    constructorId INTEGER PRIMARY KEY AUTOINCREMENT,
    constructorRef TEXT,
    name TEXT ,
    nationality TEXT ,
    url TEXT);""")
print('table with indice created successfully')

# create indices:
createSecondaryIndexconstructors = "CREATE INDEX IF NOT EXISTS index_constructors2 ON constructors2 (constructorId)"
c2.execute(createSecondaryIndexconstructors)

# open csv file constructors
const2 = open('constructors.csv')

with const2 as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin)  # comma is default delimiter
    to_db2 = [(i['constructorId'], i['constructorRef'], i['name'], i['nationality'], i['url']) for i in dr]
# insert data from csv file "constructors" to table constructors
c2.executemany(
    "INSERT INTO constructors2 ('constructorId', 'constructorRef', 'name', 'nationality', 'url') VALUES (?, ?, ?, ?, ?);",
    to_db2)

# SQL query to retrieve all data from the constructor table To verify that the data of the csv file has been successfully inserted into the table
select_2 = "SELECT constructorId, name, nationality FROM constructors2"
rows2 = c2.execute(select_2).fetchall()

#Output to the console screen
for r in rows2:
    print(r)

# create table constructor_standings
c2.execute("""CREATE TABLE standings2 (
    constructorStandingsId INTEGER PRIMARY KEY AUTOINCREMENT,
    raceId TEXT,
    constructorId INTEGER ,
    points INTEGER ,
    position FLOAT ,
    positionText TEXT,
    wins INTEGER ,
    FOREIGN KEY (constructorId)
       REFERENCES constructors (constructorId));""")
print('table created with indice successfully')

#create indice:
createSecondaryIndexstandings = "CREATE INDEX IF NOT EXISTS index_standings2 ON standings2 (constructorStandingsId)"
c2.execute(createSecondaryIndexstandings)

# open csv file standings
stand2 = open('constructor_standings.csv')

with stand2 as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr_s = csv.DictReader(fin)  # comma is default delimiter
    to_db_s2 = [(i['constructorStandingsId'], i['raceId'], i['constructorId'], i['points'], i['position'],
                i['positionText'], i['wins']) for i in dr_s]
# insert data from csv file "constructors" to table constructors
c2.executemany(
    "INSERT INTO standings2 ('constructorStandingsId', 'raceId', 'constructorId', 'points', 'position', 'positionText','wins') VALUES (?, ?, ?, ?, ?, ?, ?) ;",
    to_db_s2)

# SQL query to retrieve all data from the standings table To verify that the data of the csv file has been successfully inserted into the table
select_4 = "SELECT constructorId, wins FROM standings2"
rows_s2 = c2.execute(select_4).fetchall()

# Output to the console screen
for r in rows_s2:
    print(r)

# create table races
c2.execute("""CREATE TABLE races2 (
    raceId INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER,
    constructorId INTEGER ,
    round INTEGER,
    circuitId INTEGER ,
    name TEXT,
    date DATE,
    time TEXT,
    url TEXT,
    fp1_date TEXT,
    fp1_time TEXT,
    fp2_date TEXT,
    fp2_time TEXT,
    fp3_date TEXT,
    fp3_time TEXT,
    quali_date TEXT,
    quali_time TEXT,
    sprint_date TEXT,
    sprint_time TEXT,
    FOREIGN KEY (raceId)
       REFERENCES constructors (constructorId))""")
print('table created with indice successfully')

#create indice:
createSecondaryIndexraces = "CREATE INDEX IF NOT EXISTS index_races2 ON races2 (raceId)"
c2.execute(createSecondaryIndexraces)

# open csv file constructors_results
race2 = open('races.csv')

with race2 as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr_r = csv.DictReader(fin)  # comma is default delimiter
    to_db_r2 = [(i['raceId'], i['year'], i['round'], i['circuitId'], i['name'], i['date'], i['time'], i['url'], i['fp1_date'], i['fp1_time'], i['fp2_date'], i['fp2_time'], i['fp3_date'], i['fp3_time'], i['quali_date'], i['quali_time'], i['sprint_date'], i['sprint_time']) for i in dr_r]
# insert data from csv file "constructors_results" to table constructors_results
c2.executemany(
    "INSERT INTO races2 ('raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time', 'url',"
    " 'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time', 'quali_date', 'quali_time', 'sprint_date', "
    "'sprint_time' ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?) ;",
    to_db_r2)

# SQL query to retrieve all data from the constructor table To verify that the data of the csv file has been successfully inserted into the table
select_6 = "SELECT raceId, year FROM races2"
rows_r2 = c2.execute(select_6).fetchall()

# Output to the console screen
for r in rows_r2:
    print(r)

# Query for INNER JOIN with indice
sql2 = "SELECT standings2.wins AS wins, constructors2.name, constructors2.nationality AS nationality, races2.raceId " \
       "AS raceID, races2.year AS year FROM constructors2 INNER JOIN standings2 ON constructors2.constructorId = " \
       "standings2.constructorId INNER JOIN races2 ON standings2.raceId = races2.raceId GROUP BY wins ORDER BY CAST(" \
       "wins AS INTEGER) DESC, CAST(year AS INTEGER) DESC"

result2 = c2.execute(sql2)
# Executing the query

for a in result2:
    print(a)

#testing performance
query3 = "SELECT name FROM constructors2 WHERE name LIKE '%O'"

time_i2 = time.time()
c2.execute(query3)
records_query3 = c2.fetchall()
time_f2 = time.time()
print('time with index : ', time_f2-time_i2)

query4 = "SELECT wins FROM standings2 WHERE wins = 10"

time_i2 = time.time()
c2.execute(query4)
records_query4 = c2.fetchall()
time_f2 = time.time()
print('time with index : ', time_f2-time_i2)
