import csv
import sqlite3
import time
import pandas as pd

# create a database named formula1
df_const = pd.read_csv('constructors.csv')
df_stand = pd.read_csv('constructor_standings.csv')
df_results = pd.read_csv('races.csv')
conn = sqlite3.connect('results.db')
c = conn.cursor()

#create table constructors with no indice
c.execute("""CREATE TABLE IF NOT EXISTS  constructors (
    constructorId INTEGER PRIMARY KEY AUTOINCREMENT,
    constructorRef TEXT,
    name TEXT ,
    nationality TEXT ,
    url TEXT);""")
print('table with no indice created successfully')

# open csv file constructors
const = open('constructors.csv')

with const as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin)  # comma is default delimiter
    to_db = [(i['constructorId'], i['constructorRef'], i['name'], i['nationality'], i['url']) for i in dr]
# insert data from csv file "constructors" to table constructors
c.executemany(
    "INSERT INTO constructors ('constructorId', 'constructorRef', 'name', 'nationality', 'url') VALUES (?, ?, ?, ?, ?);",
    to_db)

# SQL query to retrieve all data from the constructor table To verify that the data of the csv file has been successfully inserted into the table
select_1 = "SELECT constructorId, name, nationality FROM constructors"
rows = c.execute(select_1).fetchall()

# Output to the console screen
for r in rows:
    print(r)

# create table constructor_standings with no indice
c.execute("""CREATE TABLE standings (
    constructorStandingsId INTEGER PRIMARY KEY AUTOINCREMENT,
    raceId TEXT,
    constructorId INTEGER ,
    points INTEGER ,
    position FLOAT ,
    positionText TEXT,
    wins INTEGER ,
    FOREIGN KEY (constructorId)
       REFERENCES constructors (constructorId));""")
print('table created with no indice successfully')

# open csv file standings
stand = open('constructor_standings.csv')

with stand as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr_s = csv.DictReader(fin)  # comma is default delimiter
    to_db_s = [(i['constructorStandingsId'], i['raceId'], i['constructorId'], i['points'], i['position'],
                i['positionText'], i['wins']) for i in dr_s]
# insert data from csv file "constructors" to table constructors
c.executemany(
    "INSERT INTO standings ('constructorStandingsId', 'raceId', 'constructorId', 'points', 'position', 'positionText','wins') VALUES (?, ?, ?, ?, ?, ?, ?) ;",
    to_db_s)


# SQL query to retrieve all data from the standings table To verify that the data of the csv file has been successfully inserted into the table
select_3 = "SELECT constructorId, wins FROM standings"
rows_s = c.execute(select_3).fetchall()

#Output to the console screen
for r in rows_s:
    print(r)

# create table races without indice
c.execute("""CREATE TABLE races (
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
print('table created with no indice successfully')

# open csv file constructors_results
race = open('races.csv')

with race as fin:  # `with` statement available csv.DictReader uses first line in file for column headings by default
    dr_r = csv.DictReader(fin)  # comma is default delimiter
    to_db_r = [(i['raceId'], i['year'], i['round'], i['circuitId'], i['name'], i['date'], i['time'], i['url'], i['fp1_date'], i['fp1_time'], i['fp2_date'], i['fp2_time'], i['fp3_date'], i['fp3_time'], i['quali_date'], i['quali_time'], i['sprint_date'], i['sprint_time']) for i in dr_r]
# insert data from csv file "constructors_results" to table constructors_results
c.executemany(
    "INSERT INTO races ('raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time', 'url',"
    " 'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time', 'quali_date', 'quali_time', 'sprint_date', "
    "'sprint_time' ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?) ;",
    to_db_r)

# SQL query to retrieve all data from the constructor table To verify that the data of the csv file has been successfully inserted into the table
select_5 = "SELECT raceId, year FROM races"
rows_r = c.execute(select_5).fetchall()

# Output to the console screen
for r in rows_r:
    print(r)

# Query for INNER JOIN without indice
sql1 = "SELECT standings.wins AS wins, constructors.name, constructors.nationality AS nationality, races.raceId AS " \
       "raceID, races.year AS year FROM constructors INNER JOIN standings ON constructors.constructorId = " \
       "standings.constructorId INNER JOIN races ON standings.raceId = races.raceId GROUP BY wins ORDER BY CAST(wins " \
       "AS INTEGER) DESC, CAST(year AS INTEGER) DESC "
result1 = c.execute(sql1)

for a in result1:
    print(a)

#testing performance
query1 = "SELECT name FROM constructors WHERE name LIKE '%O'"

time_i = time.time()
c.execute(query1)
records_query1 = c.fetchall()
time_f = time.time()
print('time with no index: ', time_f-time_i)

query2 = "SELECT wins FROM standings WHERE wins = 10"

time_i = time.time()
c.execute(query2)
records_query2 = c.fetchall()
time_f = time.time()
print('time with no index: ', time_f-time_i)

c.close()

