import json
import os
import re

import psycopg2
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
curs = conn.cursor()

curs.execute(
    '''SELECT original_cast FROM show_urls;'''
)
cast_urls = curs.fetchall()
# curs.execute(
#     '''CREATE TABLE casts
#     (cast_url VARCHAR PRIMARY KEY,
#     original_cast VARCHAR,
#     current_cast VARCHAR);'''
# )

for url in cast_urls:
    html = requests.get(url[0])
    soup = BeautifulSoup(html.content, "html.parser")
    grid = soup.find(class_="row persons-list grid")
    names_html = grid.find_all(class_="name")
    names = [re.sub("'", "", name.text) for name in names_html]

    current = re.sub("cast", "current-cast", url[0])
    soup = BeautifulSoup(html.content, "html.parser")
    grid = soup.find(class_="row persons-list grid")
    current_names_html = grid.find_all(class_="name")
    current_names = [re.sub("'", "", name.text) for name in current_names_html]
    # curs.execute(
    #     f'''INSERT INTO casts
    #     (cast_url, original_cast, current_cast)
    #     VALUES ('{url[0]}', '{json.dumps(names)}', '{json.dumps(current_names)}');'''
    # )
#
# conn.commit()
conn.close()
