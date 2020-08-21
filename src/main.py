import gzip
import json
import sqlite3
from os import path

import pandas as pd

class JsonToSqliteLoader:
    def __init__(self, file_path, dbCnx):
        self.path = file_path
        self.fileName = path.basename(file_path)
        self.tableName = self.fileName.split(".")[0].replace("-", "_")
        self.dbCnx = dbCnx
        self.setupTable()

    def setupTable(self):
        self.createTable()
        self.createIndex()

    def createTable(self):
        # Infelizmente o SQLite não suporta parametrizar o nome da tabela
        query = '''
            CREATE TABLE IF NOT EXISTS {}
            (
                  anonymous_id VARCHAR
                , browser_family VARCHAR
                , os_family VARCHAR
                , device_family VARCHAR
                , device_sent_timestamp BIGINT
                , source_file VARCHAR
            )
        '''.format(self.tableName)

        cur = self.dbCnx.cursor()
        cur.execute(query)
        self.dbCnx.commit()

    def createIndex(self):
        query = '''
            CREATE INDEX "{}_user_idx" ON "{}" (
	            "anonymous_id"
            )
        '''.format(self.tableName, self.tableName)

        cur = self.dbCnx.cursor()
        cur.execute(query)
        self.dbCnx.commit()

    def readLines(self):
        with gzip.open(self.path) as f:
            for line in f:
                yield json.loads(line)

    def insertByChunks(self, trxSize=250000):
        def getChunk(chunkSize):
            accumulator = []
            i = 0
            for jsonObj in self.readLines():
                accumulator.append(jsonObj)
                i += 1
                if i == chunkSize:
                    yield accumulator
                    accumulator = []
                    i = 0
            yield accumulator

        columns = ["anonymous_id", "browser_family", "os_family", "device_family", "device_sent_timestamp"]
        insertQuery = '''
            INSERT INTO {}
                (
                      anonymous_id
                    , browser_family
                    , os_family
                    , device_family
                    , device_sent_timestamp
                    , source_file
                )
            VALUES (?, ?, ?, ?, ?, ?)
        '''.format(self.tableName)

        cur = self.dbCnx.cursor()

        for chunk in getChunk(trxSize): 
            cur.execute("BEGIN TRANSACTION")
            for jsonObj in chunk:
                cur.execute(insertQuery, (*(jsonObj[col] for col in columns), self.fileName))
            cur.execute("COMMIT")

def getTables(db):
    cur = db.cursor()
    cur.execute("select name from sqlite_master where type = 'table'")
    for res in cur:
        yield res[0]

def getUsersInTable(db, table):
    cur = db.cursor()
    query = "select distinct anonymous_id from {}".format(table)
    cur.execute(query)
    for res in cur:
        yield res

def getEventsByUser(db, table, userId):
    query = '''
        select anonymous_id
             , browser_family
             , os_family
             , device_family
             , device_sent_timestamp
             , source_file
        from {}
        where anonymous_id = ?
    '''.format(table)

    df = pd.read_sql(query, db, params=userId)

    return df

class session:
    def __init__(self, anonymous_id, browser_family, os_family, device_family, source_file, duration):
        self.anonymous_id = anonymous_id
        self.browser_family = browser_family
        self.os_family = os_family
        self.device_family = device_family
        self.source_file = source_file
        self.duration = duration

    
def sessionEvents(events):
    return 0

def insertSessions(db, table, sessions):
    pass


def main():
    db = sqlite3.connect("../db/raw_data.sqlite3")
    # for i in range(1,10):
    #     reader = JsonToSqliteLoader(f"/mongodata/part-{i:05}.json.gz", db)
    #     reader.insertByChunks()
    for table in getTables(db):
        userIds = getUsersInTable(db, table)
        print(f"Found all users in table {table}")
        i = 0
        for userId in userIds:
            print(f"Fetching events for user {userId}")
            userEvents = getEventsByUser(db, table, userId)
            sessions = sessionEvents(userEvents)
            insertSessions(db, table, sessions)
            print(f"Got'em: {userEvents.shape[0]} records")
            userEvents.head()
            if i == 99:
                break
            i+=1


    return 0
           


if __name__ == "__main__":
    main()