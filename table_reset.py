import sqlite3

conn = sqlite3.connect('taskdb')
cur = conn.cursor()

cur.execute("drop table player")
cur.execute("create table player(url, name, fullname, birth_date, age, birth_place, birth_country, positions, current_club, national_team, appearances_current_club, goals_current_club, scraping_timestamp)")
conn.commit()
