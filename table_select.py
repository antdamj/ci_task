import sqlite3

conn = sqlite3.connect('taskdb')
cur = conn.cursor()

data = cur.execute("select * from player").fetchall()
for row in data:
    print(row)
