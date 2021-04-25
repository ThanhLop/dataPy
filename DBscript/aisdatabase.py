import sqlite3

def create_table():
    conn=sqlite3.connect("AisDatabase.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS AisData (MMSI INTEGER, 'Ship name' TEXT, 'Call sign' TEXT, 'IMO number' REAL, 'Ship type' TEXT, Destination TEXT, 'Maximum actual draught' REAL, Cargo TEXT, Latitude REAL, Longitude REAL, 'Unix time' INTEGER, SOG REAL, COG REAL, Heading REAL, 'Nav Status' TEXT, ROT REAL, A REAL, B REAL, C REAL, D REAL, 'Time stamp' TEXT, 'Expected time' TEXT, 'Signal type' TEXT, Ais TEXT, Project TEXT, Country TEXT, Area TEXT, Source TEXT, Directory TEXT)")
    conn.commit()
    conn.close()

create_table()
