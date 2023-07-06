from bs4 import BeautifulSoup
import datetime
import json
import pandas as pd
import re
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException,
                                        WebDriverException)
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from threading import Thread
from time import sleep

date_formats = {
    '%d/%m/%Y %H:%M:%S': re.compile(r','),
    '%m/%d/%Y %H:%M:%S': None,
}

cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)


def process_times():
    # this function retrieves fixtures from a Mysql database and deletes those that have been played out.

    username = os.getenv('JAWSDB_USERNAME')
    password = os.getenv('JAWSDB_PASSWORD')
    hostname = os.getenv('JAWSDB_HOST')
    port = os.getenv('JAWSDB_PORT')
    database = os.getenv('JAWSDB_DATABASE')

    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()

    query = "SELECT delete_time FROM fixture_database"
    cursor.execute(query)
    now = datetime.now()

    for row in cursor:

        delete_time_str = row[0]

        try:
            delete_time = datetime.datetime.strptime(delete_time_str, '%d/%m/%Y %H:%M')
        except ValueError:
            delete_time = datetime.datetime.strptime(delete_time_str, '%m/%d/%Y %H:%M')

        if now > delete_time:
            query = "DELETE FROM fixture_database WHERE delete_time=%s"
            cursor.execute(query, (delete_time_str,))
            cnx.commit()

    cursor.close()
    cnx.close()


process_times.cache = {}


def check_ids(mid):
    # this function checks if a fixture id from flashscore.com is already in our database or not. If it is, we skip.
    username = os.getenv('JAWSDB_USERNAME')
    password = os.getenv('JAWSDB_PASSWORD')
    hostname = os.getenv('JAWSDB_HOST')
    port = os.getenv('JAWSDB_PORT')
    database = os.getenv('JAWSDB_DATABASE')

    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()

    query = "SELECT id FROM fixture_database"
    cursor.execute(query)
    rows = cursor.fetchall()
    if len(rows) == 0:
        return True
    for row in rows:
        iid = row[0]
        if iid != mid:
            return True
        else:
            return False


def get_match_info(driver, country, league, mid):
    # this function navigate to the fixture page and scrapes team names, kick off time, referee, and other stats.
    url = f"https://www.flashscore.com/match/{mid}/#/match-summary"
    driver.get(url)

    sleep(0.25)

    html = driver.page_source
    soup = BeautifulSoup(html, "html")
    today = datetime.now()
    kickoff = soup.find("div", class_="duelParticipant__startTime").text
    kickoff = kickoff.replace(".", "/")

    try:
        match_date = datetime.strptime(kickoff, '%d/%m/%Y %H:%M')
    except:
        match_date = datetime.strptime(kickoff, '%m/%d/%Y %H:%M')

    if not (match_date - today).total_seconds() < (60 * 60 * 72) and ((match_date - today).total_seconds()) > 0:
        return None

    deletion_time = (match_date + timedelta(hours=2))

    try:
        delete_time = datetime.strftime(deletion_time, '%d/%m/%Y %H:%M')
    except:
        delete_time = datetime.strptime(deletion_time, '%m/%d/%Y %H:%M')

    return [mid, ht, at, kickoff, country, league, delete_time]


def get_fixture_list():
    # This function scrapes the fixtures due in the next 72 hours in the leagues covered in soccer_countries_leagues.
    # It saves the fixture info into a mysql table for later use by our form scraper.

    chrome_options = ChromeOptions()
    chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)

    username = os.getenv('JAWSDB_USERNAME')
    password = os.getenv('JAWSDB_PASSWORD')
    hostname = os.getenv('JAWSDB_HOST')
    port = os.getenv('JAWSDB_PORT')
    database = os.getenv('JAWSDB_DATABASE')

    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()
    query = "SELECT * FROM soccer_countries_leagues"
    cursor.execute(query)

    fixture_rows = []
    for row in cursor:
        country = row[0]
        for i in range(1, 9):
            league = row[i]
            if league == "None":
                continue
            process_times(country, league)
            mids = get_ids(country, league, driver)
            for mid in mids:
                if check_ids(mid):
                    match_row = get_match_info(driver, country, league, mid)
                    if match_row is None:
                        break
                    fixture_rows.append(match_row)

    cursor.close()
    cnx.close()

    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()

    for row in fixture_rows:
        query = "INSERT INTO fixture_database" +
        "(id, home, away, kickoff, country, league, delete_time)" +
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, row)


cursor.close()
cnx.commit()

get_fixture_list()
