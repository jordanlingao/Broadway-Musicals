import os
import re

from dotenv import load_dotenv
import psycopg2
import requests
from bs4 import BeautifulSoup
from data.show_urls import BASE_URL
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.common import TimeoutException
import time
from psycopg2.extensions import AsIs

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"))

curs = conn.cursor()
curs.execute(
    '''
    SELECT title, grosses from show_urls
    WHERE grosses != 'None';
    '''
)

grosses_urls = curs.fetchall()

# Replace invalid urls
GROSSES_BASE_URL = "https://www.broadwayworld.com/grosses.cfm"
base_html = requests.get(GROSSES_BASE_URL)
base_soup = BeautifulSoup(base_html.content, "html.parser")
base_href = [(html.text.lower(), html.get("href")) for html in base_soup.find_all(class_="title")]

for ind, item in enumerate(grosses_urls):
    check_html = requests.get(item[1])
    check_soup = BeautifulSoup(check_html.content, "html.parser")
    h1 = check_soup.find("h1")
    if "Broadway Grosses" in h1.text or "Not Found" in h1.text:
        title = item[0].lower()
        for href in base_href:
            if title in href[0]:
                curs.execute(
                    f'''
                    UPDATE show_urls
                    SET grosses = '{BASE_URL + href[1]}'
                    WHERE title = '{title}';
                    '''
                )
                conn.commit()
                grosses_urls[ind] = (title, BASE_URL + href[1])
                break

curs.execute(
    '''
    CREATE TABLE IF NOT EXISTS grosses
    (title VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    week_ending VARCHAR NOT NULL,
    gross_difference FLOAT,
    gross_difference_percent FLOAT,
    gross_profit FLOAT,
    week_number INT,
    total_attendance INT,
    percent_capacity FLOAT,
    total_capacity INT,
    performance_count INT,
    avg_ticket_price FLOAT,
    top_ticket_price FLOAT,
    PRIMARY KEY (title, week_ending));
    '''
)
conn.commit()

for url in grosses_urls:
    driver = webdriver.Chrome()
    driver.get(url[1])
    wait = WebDriverWait(driver, 5)

    # Following code from Stack Overflow
    # https://stackoverflow.com/questions/76302763/having-trouble-getting-html-from-after-a-button-is-clicked-webdriverwait-is-timi
    # Asked by me: https://stackoverflow.com/users/21909422/jordan-lingao
    # Answered by https://stackoverflow.com/users/7736228/abhay-chaudhary
    # Click out of ad
    try:
        ad_button = wait.until(
            expected_conditions.visibility_of_element_located((By.XPATH, '//a[text()="AD - CLICK HERE TO CLOSE"]')))
        ad_button.click()
    except TimeoutException:
        pass

    # Scroll
    show_all_button = driver.find_element(By.XPATH, '//button[text()="Show All"]')
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_all_button)
    time.sleep(2)

    # Click out of ad
    try:
        ad_button = wait.until(
            expected_conditions.visibility_of_element_located((By.XPATH, '//a[text()="AD - CLICK HERE TO CLOSE"]')))
        ad_button.click()
    except TimeoutException:
        pass

    show_all_button = driver.find_element(By.XPATH, '//button[text()="Show All"]')

    # Wait for button to be clickable
    time.sleep(5)
    show_all_button.click()

    # Wait for new table to load
    time.sleep(5)
    # End snippet from Stack Overflow

    # Get table html
    table = driver.find_element(By.CLASS_NAME, "table-body")
    table_html = table.get_attribute("outerHTML")
    driver.close()
    table_soup = BeautifulSoup(table_html, "html.parser")
    rows = table_soup.find_all(class_="row")

    columns = ["title", "url", "week_ending", "data-gross-diff-year", "data-diff-percents", "data-gross", "data-week-n",
               "data-attendee", "data-capacity", "capacity", "data-perform", "data-ticket", "top_ticket"]

    # Loop through rows to retrieve data
    for row in rows:
        row_values = row.attrs
        row_values["week_ending"] = row.find(class_='cell').text
        top_and_capacity = table_soup.find_all('span', class_='in')[:2]
        if len(top_and_capacity) == 2:
            row_values["top_ticket"] = float(re.sub(r"\$", "", top_and_capacity[0].get_text()))
            row_values["capacity"] = int(re.sub(r"[^0-9]", "", top_and_capacity[1].get_text()))
        for column in columns:
            if column not in row_values:
                row_values[column] = AsIs("NULL")

        curs.execute(
            f'''
            INSERT INTO grosses (title, url, week_ending, gross_difference, gross_difference_percent, gross_profit,
            week_number, total_attendance, percent_capacity, total_capacity, performance_count, avg_ticket_price,
            top_ticket_price)
            VALUES
            ('{url[0]}', '{url[1]}', '{row_values["week_ending"]}', {row_values["data-gross-diff-year"]},
            {row_values["data-diff-percents"]}, {row_values["data-gross"]}, {row_values["data-week-n"]},
            {row_values["data-attendee"]}, {row_values["data-capacity"]}, {row_values["capacity"]},
            {row_values["data-perform"]}, {row_values["data-ticket"]}, {row_values["top_ticket"]});
            '''
        )
conn.commit()
conn.close()
