import os

import requests
from bs4 import BeautifulSoup
import psycopg2
from dotenv import load_dotenv
from collections import defaultdict
import re

html = requests.get('https://www.broadwayworld.com/shows/broadway-shows.php')
soup = BeautifulSoup(html.content, 'html.parser')

# Target the rows of the list of shows
soup_rows = soup.findAll('div', class_='d-flex')

# Target each show in the row
soup_shows = []
for html in soup_rows:
    shows_row = html.findAll('div', class_='ticket-single')
    for show in shows_row:
        soup_shows.append(show)

# Target the right href for each show and add it to the base url
show_urls = []
BASE_URL = "https://www.broadwayworld.com/"

for html in soup_shows:
    html = html.find("a")
    href = html.get("href")
    show_urls.append(BASE_URL + href)

# Pop comedy show from list
show_urls.remove('https://www.broadwayworld.com//shows/Alex-Edelman-Just-For-Us-334473.html')


def get_show_urls(show_urls):
    info = defaultdict(list)
    for url in show_urls:
        html = requests.get(url)
        soup = BeautifulSoup(html.content, "html.parser")
        info["title"].append(soup.find("h1").text)

        run_time_text = soup.find(class_="timing")
        if run_time_text:
            run_time_text = run_time_text.find("span").find("span").text
            run_time_text = re.sub(",.+", "", run_time_text)
            run_time_text = re.sub("with.+", "", run_time_text)
            run_time_text = re.sub("including.+", "", run_time_text).strip()
            info["run_time"].append(run_time_text)
        else:
            info["run_time"].append(None)

        top_text = soup.find(class_="col-lg-5 col-md-5 px-0")
        opened = top_text.find("span", attrs={"style": "color:white;"}).text
        info["opened"].append(opened)
        cast = soup.find(class_="flex-sm-fill text-start cast-tab").get("href").replace("current-", "")
        info["original_cast"].append(BASE_URL+cast)
        info["creative_team"].append(BASE_URL+soup.find(class_="flex-sm-fill text-start creative-tab").get("href"))
        grosses = BASE_URL+soup.find(class_="flex-sm-fill text-start grosses-tab").get("href")
        if grosses == "https://www.broadwayworld.com//grosses/":
            info["grosses"].append(None)
        else:
            info["grosses"].append(grosses)
        info["show_history"].append(BASE_URL + soup.find(class_="flex-sm-fill text-start show-tab").get("href"))
        reviews = soup.find(class_="flex-sm-fill text-start reviews-tab")
        if reviews:
            info["reviews"].append(BASE_URL + reviews.get("href"))
        else:
            info["reviews"].append(None)
        photos = soup.find(class_="flex-sm-fill text-start photos-tab")
        if photos:
            info["photos"].append(BASE_URL + photos.get("href"))
        else:
            info["photos"].append(None)
    return info


if __name__ == "__main__":
    info = get_show_urls(show_urls)
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    curs = conn.cursor()
    # curs.execute(
    #     '''CREATE TABLE show_urls
    #     (id SERIAL PRIMARY KEY,
    #     url VARCHAR NOT NULL,
    #     title VARCHAR NOT NULL,
    #     run_time VARCHAR,
    #     opened VARCHAR,
    #     original_cast VARCHAR,
    #     creative_team VARCHAR,
    #     grosses VARCHAR,
    #     show_history VARCHAR,
    #     reviews VARCHAR,
    #     photos VARCHAR);'''
    # )
    #
    # for i in range(len(show_urls)):
    #     curs.execute(
    #         f'''INSERT INTO show_urls
    #         (url, title, run_time, opened, original_cast, creative_team, grosses, show_history, reviews, photos)
    #         VALUES (
    #         '{show_urls[i]}',
    #         '{info["title"][i]}',
    #         '{info["run_time"][i]}',
    #         '{info["opened"][i]}',
    #         '{info["original_cast"][i]}',
    #         '{info["creative_team"][i]}',
    #         '{info["grosses"][i]}',
    #         '{info["show_history"][i]}',
    #         '{info["reviews"][i]}',
    #         '{info["photos"][i]}'
    #         );'''
    #     )
    # conn.commit()
    conn.close()
