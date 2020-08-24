import gzip
import json
import sqlite3
from os import path

import numpy as np

class JsonToSqliteLoader:
    def __init__(self, file_path, dbCnx):
        self.path = file_path
        self.fileName = path.basename(file_path)
        self.tableName = self.fileName.split(".")[0].replace("-", "_")
        self.dbCnx = dbCnx
        self.setupTable()

    def setupTable(self):
        self.createTable()

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
    query = '''select distinct 
          anonymous_id 
    from {}'''.format(table)
    cur.execute(query)
    for res in cur:
        yield res

def getSessionKeysInTable(db, table):
    cur = db.cursor()
    query = '''select distinct 
          anonymous_id 
        , browser_family
        , os_family
        , device_family
        , source_file
    from {}'''.format(table)
    cur.execute(query)
    for res in cur:
        yield res

def getEventsByUserKey(db, table, userKey):
    query = '''
        select browser_family
            , os_family
            , device_family
            , source_file
            ,device_sent_timestamp
        from {}
        where anonymous_id = ?
    '''.format(table)

    cur = db.cursor()
    cur.execute(query, userKey)

    for res in cur:
        yield res

def getEventsBySessionKeys(db, table, sessionKeys):
    query = '''
        select device_sent_timestamp
        from {}
        where anonymous_id = ?
            and browser_family = ?
            and os_family = ?
            and device_family = ?
            and source_file = ?
    '''.format(table)

    cur = db.cursor()
    cur.execute(query, sessionKeys)

    for res in cur:
        yield res[0]



def sessionEvents(events, timeout=30*60*1000):
    prev = 0
    duration = 0
    i = 0
    sessions = []

    events = np.array([*events])
    N = events.size

    if N == 1:
        return [0]

    events.sort()
    while i < N:
        prev = events[i]
        lookahead = prev + timeout
        new_i = events.searchsorted(lookahead)
        
        # There's no other value smaller or equal to lookahead
        # ie this session is over
        if  new_i - i == 1 and (new_i >= N or events[new_i] > lookahead):
            sessions.append(duration)
            duration = 0
            i = new_i
            continue
        elif new_i - i > 1:
            new_i -= 1

        i = new_i
        duration += events[i] - prev

    return sessions

class SessionDbInserter:

    tableName = "SESSIONS"

    def __init__(self, db):
        self.db = db
        self.createTable()

    def createTable(self):
        query = '''
            CREATE TABLE IF NOT EXISTS {} (
                  anonymous_id VARCHAR
                , browser_family VARCHAR
                , os_family VARCHAR
                , device_family VARCHAR
                , source_file VARCHAR
                , duration INT
            )
        '''.format(SessionDbInserter.tableName)
        cur = self.db.cursor()
        cur.execute(query)
        self.db.commit()

    def createIndexes(self):
        columns = ["browser_family"
                 , "os_family"
                 , "device_family"
                 , "source_file"]
        query = '''
            CREATE INDEX IF NOT EXISTS "{col}_idx" ON "{table}" (
	            "{col}"
            )
        '''
        cur = self.db.cursor()
        for column in columns:
            cur.execute(query.format(col=column, table=SessionDbInserter.tableName))
            self.db.commit()
    
    def bulkInsertSessions(self, sessions, trxSize=250000):
        def getChunk(chunkSize):
            accumulator = []
            i = 0
            for session in sessions:
                accumulator.append(session)
                i += 1
                if i == trxSize:
                    yield accumulator
                    i = 0


            yield accumulator

        insertQuery = '''
            INSERT INTO {}
                (
                      anonymous_id
                    , browser_family
                    , os_family
                    , device_family
                    , source_file
                    , duration
                )
            VALUES (?, ?, ?, ?, ?, ?)
        '''.format(self.tableName)

        cur = self.db.cursor()

        for chunk in getChunk(trxSize): 
            cur.execute("BEGIN TRANSACTION")
            for session in chunk:
                cur.execute(insertQuery, session)
            cur.execute("COMMIT")

    
def getSessionsFromTable(db, table):
    sessionKeys = getSessionKeysInTable(db, table)
    for sessionKey in sessionKeys:
            events = getEventsBySessionKeys(db, table, sessionKey)
            sessionDurations = sessionEvents(events)
            for duration in sessionDurations:
                yield (*sessionKey, duration)

class QuestionSolutions:
    def __init__(self, sessionsDb):
        self.db = sessionsDb
        self.__browserFamilies = self.getDistinct("browser_family")
        self.__osFamilies = self.getDistinct("os_family")
        self.__deviceFamilies = self.getDistinct("device_family")
        self.__sourceFiles = self.getDistinct("source_file")

    def getDistinct(self, attrName):
        query = '''
            SELECT DISTINCT {}
            FROM SESSIONS
        '''
        res = self.db.execute(query.format(attrName))

        return [x[0] for x in res.fetchall()]

    def getSessionCount(self, sliceField, sliceValues):
        query = '''
            SELECT COUNT(*)
            FROM SESSIONS
            WHERE {} = ?
        '''
        result = {}

        for value in sliceValues:
            res = self.db.execute(query.format(sliceField), (value,))
            result[value] = res.fetchone()[0]

        return result

    def getSessionDurations(self, sliceField, sliceValues):
        query = '''
            SELECT DURATION
            FROM SESSIONS
            WHERE {} = ?
        '''
        result = {}

        for value in sliceValues:
            res = self.db.execute(query.format(sliceField), (value,))
            result[value] = [x[0] for x in res.fetchall()]

        return result

    def getSessionMedians(self, sliceField, sliceValues):
        durationsDict = self.getSessionDurations(sliceField, sliceValues)

        for k, v in durationsDict.items():
            durationsArray = np.array(v).astype(np.int32)
            durationsDict[k] = np.median(durationsArray)

        return durationsDict

    def questao1(self):
        sessionCounts = self.getSessionCount("source_file", self.__sourceFiles)

        return sessionCounts

    def questao2(self):
        returnObj = {}

        returnObj["browser_family"] = self.getSessionCount("browser_family", self.__browserFamilies)
        returnObj["os_family"] = self.getSessionCount("os_family", self.__osFamilies)
        returnObj["device_family"] = self.getSessionCount("device_family", self.__deviceFamilies)

        return returnObj

    def questao3(self):
        returnObj = {}

        returnObj["browser_family"] = self.getSessionMedians("browser_family", self.__browserFamilies)
        returnObj["os_family"] = self.getSessionMedians("os_family", self.__osFamilies)
        returnObj["device_family"] = self.getSessionMedians("device_family", self.__deviceFamilies)

        return returnObj


def main():
    rawDataDb = sqlite3.connect("./db/raw_data.sqlite3")
    sessionDb = sqlite3.connect("./db/sessions.sqlite3")
    sessionInserter = SessionDbInserter(sessionDb)

    for i in range(1,10):
        dataFile = f"./data/part-{i:05}.json.gz"
        reader = JsonToSqliteLoader(dataFile, rawDataDb)
        reader.insertByChunks()
        reader.createIndex()
    for table in getTables(rawDataDb):
        print(f"Processing sessions for table {table}")
        sessions = getSessionsFromTable(rawDataDb, table)
        sessionInserter.bulkInsertSessions(sessions)
    sessionInserter.createIndexes()

    solutions = QuestionSolutions(sessionDb)
    q1 = solutions.questao1()
    print(json.dumps(q1, indent=4))
    q2 = solutions.questao2()
    print(json.dumps(q2, indent=4))
    q3 = solutions.questao3()
    print(json.dumps(q3, indent=4))
           


if __name__ == "__main__":
    main()