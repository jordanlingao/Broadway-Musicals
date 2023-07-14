import json
import os
import re

import psycopg2
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from data.show_urls import BASE_URL
from psycopg2.extensions import AsIs

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
curs = conn.cursor()

curs.execute('''SELECT title, show_history FROM show_urls''')
history = curs.fetchall()

curs.execute(
    '''CREATE TABLE IF NOT EXISTS history
    (url VARCHAR PRIMARY KEY,
    title VARCHAR,
    previews VARCHAR,
    opening VARCHAR,
    theatres VARCHAR,
    production_type VARCHAR,
    run_type VARCHAR,
    market VARCHAR,
    run_time VARCHAR,
    show_type VARCHAR,
    website VARCHAR,
    other_productions VARCHAR,
    nominated VARCHAR,
    won VARCHAR);'''
)
conn.commit()

for url in history:
    html = requests.get(url[1])
    soup = BeautifulSoup(html.content, "html.parser")

    # show information
    show_information = soup.find(class_="production-info production-table")
    theatres = json.dumps([item.text for item in show_information.find_all("a") if "theatre" in item.text.lower()])
    show_information = [td.text.strip("\n") for td in show_information.find_all("td")]

    info_dict = {"preview": AsIs("NULL"), "opening": AsIs("NULL"),
                 "production type": AsIs("NULL"), "run type": AsIs("NULL"), "market": AsIs("NULL"),
                 "time": AsIs("NULL"), "show_type": AsIs("NULL"), "website": AsIs("NULL")}
    for i in range(len(show_information)):
        for key in info_dict:
            if key in show_information[i].lower():
                value = show_information[i + 1]
                if value != "":
                    info_dict[key] = re.sub("'", "", value)
                break

    info_dict["theatres"] = re.sub("'", "", theatres)

    # Other Productions
    other_prods = soup.find_all(class_="production-table")[1]
    other_prods_list = []
    for tr in other_prods.find_all("tr"):
        prod = []
        for td in tr.find_all("td"):
            prod.append(td.text.strip("\n"))
        prod.append(BASE_URL + tr.find("a").get("href"))
        other_prods_list.append(prod)
    other_prods_json = re.sub("'", "", json.dumps(other_prods_list))

    # Awards
    awards_table = soup.find(class_="score-table database")
    if awards_table:
        nominated = {"year": [], "ceremony": [], "category": [], "nominee": []}
        won = {"year": [], "ceremony": [], "category": [], "nominee": []}
        awards = awards_table.find_all("tr")[1:]
        winners = awards_table.find_all(class_="winner")
        for award in awards:
            details = award.find_all("td")
            nominated["year"].append(details[0].text)
            nominated["ceremony"].append(details[1].text)
            nominated["category"].append(details[2].text)
            nominated["nominee"].append(details[3].text)

        for award in winners:
            details = award.find_all("td")
            won["year"].append(details[0].text)
            won["ceremony"].append(details[1].text)
            won["category"].append(details[2].text)
            won["nominee"].append(details[3].text)

        nominated = re.sub("'", "", json.dumps(nominated))
        won = re.sub("'", "", json.dumps(won))
    else:
        nominated = AsIs("NULL")
        won = AsIs("NULL")

    curs.execute(
        f'''INSERT INTO history
        (url, title, previews, opening, theatres, production_type, run_type, market, run_time, show_type, website,
        other_productions, nominated, won)
        VALUES (
        '{url[1]}', '{url[0]}', '{info_dict["preview"]}', '{info_dict["opening"]}', '{info_dict["theatres"]}',
        '{info_dict["production type"]}', '{info_dict["run type"]}', '{info_dict["market"]}', '{info_dict["time"]}',
        '{info_dict["show_type"]}', '{info_dict["website"]}', '{other_prods_json}', '{nominated}', '{won}');
        '''
    )
    conn.commit()

conn.close()
