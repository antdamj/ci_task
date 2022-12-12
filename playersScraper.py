from datetime import datetime
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import requests
import re
import statistics
from matplotlib import pyplot as plt
import sys


def main():

    url_file = ''
    if len(sys.argv) < 2:
        url_file = 'playersURLs.csv'
    else:
        url_file = sys.argv[1]

    # import existing data into SQL DB
    players_data = pd.read_csv('data/playersData.csv', sep=';')
    conn = sqlite3.connect('taskdb')
    cur = conn.cursor()

    for row in players_data.iterrows():
        row = row[1]

        if row['Dead'] or not row['No data']:  # eliminate rows that do not refer to actual players
            cur.execute(
                """insert into player(url, name, fullname, birth_date, age, birth_place, birth_country, positions,
                current_club, national_team, appearances_current_club, goals_current_club, scraping_timestamp)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (
                    row['URL'], row['Name'], row['Full name'], row['Date of birth'], row['Age'], row['City of birth'],
                    row['Country of birth'], row['Position'], row['Current club'], row['National_team'], '', '', ''
                )
            )

    conn.commit()

    # scrape new data off Wikipedia
    urls = []
    with open('data/' + url_file, 'r') as f:
        for line in f.readlines():
            urls.append(line.replace('\n', ''))

    urls = list(set(urls))
    updates = 0

    for url in urls:  # i.e., for each player
    # for url in []:  # i.e., skip
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')

        # all of these are fetched the same way
        classes = [
            'mw-page-title-main',  # to get name
            'nickname',  # to get full name
            'bday',  # to get birthday
            'infobox-data role',  # to get position
            'org',  # to get current team
            'ForceAgeToShow',  # to get player age
        ]
        results = []  # name, fullname, birthday, position, team, age

        for c in classes:
            player_info = None  # NULL
            find = soup.find(class_=c)
            if not isinstance(find, type(None)):
                player_info = find.text.split('[')[0].replace('\n', '')
            results.append(player_info)

        # if "(footballer*)" is in player's wiki page title
        results[0] = results[0].split(' (footballer')[0] if not isinstance(results[0], type(None)) else None
        # if there is an [1] in player's wiki page title
        results[0] = results[0].split('[')[0] if not isinstance(results[0], type(None)) else None

        # player age is written as "(age: AA)"
        results[5] = results[5][-3:-1] if not isinstance(results[5], type(None)) else None

        player_place_of_birth, player_country_of_birth = None, None
        location_of_birth = soup.find(class_='birthplace')
        if not isinstance(location_of_birth, type(None)):
            location_of_birth = location_of_birth.text.replace('\n', '').split('[')[0]
            if ',' in location_of_birth:
                player_place_of_birth, player_country_of_birth = [s.lstrip() for s in location_of_birth.rsplit(',', 1)]
            else:
                player_country_of_birth = location_of_birth

        player_national_team = soup.find(title=re.compile('national football team'))
        if not isinstance(player_national_team, type(None)):
            player_national_team = player_national_team.text.split('[')[0].replace('\n', '')
        else:
            player_national_team = None

        player_current_team_appearances, player_current_team_goals = 0, 0
        table_entries = soup.find_all('td', class_='infobox-data infobox-data-a')
        for te in table_entries:
            if results[4] and te.text and results[4] in te.text:
                player_current_team_appearances +=\
                    int(te.nextSibling.text.replace('\n', ''))\
                    if te.nextSibling.text.replace('\n', '') not in ['', '?'] else 0
                player_current_team_goals +=\
                    int(te.nextSibling.nextSibling.text.replace('\n', '').split('(')[1].split(')')[0])\
                    if te.nextSibling.nextSibling.text.replace('\n', '') not in ['', '?'] else 0

        if not player_current_team_appearances:
            player_current_team_appearances = None
        if not player_current_team_goals:
            player_current_team_goals = None

        # update database with scrapped data
        this_player = cur.execute("select * from player where name = ?", (results[0], )).fetchall()
        if len(this_player):  # player already exists, update entry
            updates += 1
            cur.execute("""update player set url = ?, name = ?, fullname = ?, birth_date = ?, age = ?, birth_place = ?,
            birth_country = ?, positions = ?, current_club = ?, national_team = ?, appearances_current_club = ?,
            goals_current_club = ?, scraping_timestamp = ? where url = ?;""", (
                url, results[0], results[1], results[2], results[5], player_place_of_birth, player_country_of_birth, results[3],
                results[4], player_national_team, player_current_team_appearances, player_current_team_goals,
                datetime.now(), url
            ))
        else:  # create new row for this player
            cur.execute(
                """insert into player(url, name, fullname, birth_date, age, birth_place, birth_country, positions,
                current_club, national_team, appearances_current_club, goals_current_club, scraping_timestamp)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (
                    url, results[0], results[1], results[2], results[5], player_place_of_birth, player_country_of_birth,
                    results[3], results[4], player_national_team, player_current_team_appearances,
                    player_current_team_goals, datetime.now(),
                )
            )
        conn.commit()

    # first query - add age_category
    if 'age_category' not in [i[1] for i in cur.execute('PRAGMA table_info(player)')]:
        cur.execute('alter table player add age_category')
    cur.execute("""
        update player set age_category =
            case
                when age <= 23 then 'Young'
                when age > 23 and age < 33 then 'MidAge'
                when age >= 33 then 'Old'
                else 'Age unknown'
            end
        """)

    # second query - add goals per match
    if 'goals_per_club_game' not in [i[1] for i in cur.execute('PRAGMA table_info(player)')]:
        cur.execute('alter table player add goals_per_club_game')
    cur.execute("""
        update player set goals_per_club_game = cast(goals_current_club as float) / appearances_current_club;
    """)

    # third query - get avg age, avg appearances, total no of players by club and visualize
    avg_stats = cur.execute("""
        select current_club, avg(age), avg(appearances_current_club) from player group by current_club having avg(appearances_current_club) > 0;
    """).fetchall()

    age_stats, appearances_stats = {}, {}
    for stat in avg_stats:
        age_stats[stat[0]] = stat[1]
        appearances_stats[stat[0]] = stat[2]

    age_stats = dict(sorted(age_stats.items(), key=lambda item: item[1]))
    appearances_stats = dict(sorted(appearances_stats.items(), key=lambda item: item[1], reverse=True))

    plt.hist(x=age_stats.values(), label=age_stats.keys())
    # plt.bar(range(len(age_stats)), list(age_stats.values()), tick_label=list(age_stats.keys()))
    # plt.xticks(rotation = 90)
    plt.savefig('age.png')

    plt.clf()

    plt.hist(x=appearances_stats.values(), label=appearances_stats.keys())
    plt.savefig('appearances.png')

    # fourth query - club players comparisons
    club = cur.execute('select distinct current_club from player').fetchall()[0][0]  # Liverpool
    player_comparisons = cur.execute("""
        select count(*) from (select * from player where current_club = ?) as p1
        join player as p2 on
        p1.age < p2.age and p1.positions = p2.positions and p1.appearances_current_club > p2.appearances_current_club;
    """, (club, )).fetchall()
    # print(player_comparisons)

    conn.commit()

    print("total updates:", updates)
    print("total table entries:", len(cur.execute("select * from player").fetchall()))
    print("average age of all players:",
          round(statistics.mean(age_stats.values()), 2))
    print("average number of appearances of all players:",
          round(statistics.mean(appearances_stats.values()), 2))
    print("median number of appearances of all players:",
          round(statistics.median(appearances_stats.values()), 2))


if __name__ == '__main__':
    main()
