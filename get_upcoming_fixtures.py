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

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from threading import Thread
from time import sleep
from tqdm import tqdm

date_formats = {
    '%d/%m/%Y %H:%M:%S': re.compile(r','),
    '%m/%d/%Y %H:%M:%S': None,
}

def process_times():
    # this function retrieves fixtures from a Mysql database and deletes those that have been played out.

    username = ?
    password = ?
    hostname = ?
    port = ?
    database = ?
    
    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()

    query = "SELECT delete_time FROM fixtures_dictionary"
    cursor.execute(query)
    now = datetime.now()

    for row in cursor:

        delete_time_str = row[0]

        try:
            delete_time = datetime.datetime.strptime(delete_time_str, '%d/%m/%Y %H:%M')
        except ValueError:
            delete_time = datetime.datetime.strptime(delete_time_str, '%m/%d/%Y %H:%M')

        if now > delete_time:
            query = "DELETE FROM fixtures_dictionary WHERE delete_time=%s"
            cursor.execute(query, (delete_time_str,))
            cnx.commit()

    cursor.close()
    cnx.close()


process_times.cache = {}


def check_ids(mid):
    # this function checks if a fixture id from flashscore.com is already in our database or not. If it is, we skip.

    username = ?
    password = ?
    hostname = ?
    port = ?
    database = ?

    cnx = mysql.connector.connect(user=username, password=password, host=hostname, port=port, database=database)
    cursor = cnx.cursor()

    query = "SELECT id FROM fixtures_dictionary"
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
    sleep(0.2)

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

    teams = soup.find_all("div", class_="participant__participantName participant__overflow")
    ht = teams[0].text
    at = teams[-1].text
    live = False

    if not soup.find("div", class_="br__broadcasts") is None:
        br = soup.find("div", class_="br__broadcasts").text
        if "(UK/Irl)" in br or "BT Sport" in br:
            live = True
    if len(soup.find_all(class_="mi__item__val")) > 1:
        ref = soup.find_all(class_="mi__item__val")[0].text
        ref = ref.split("(")
        ref = ref[0]
        ref = ref.strip()
    else:
        ref = "Null"

    deletion_time = (match_date + timedelta(hours=2))
    try:
        delete_time = datetime.strftime(deletion_time, '%d/%m/%Y %H:%M')
    except:
        delete_time = datetime.strptime(deletion_time, '%m/%d/%Y %H:%M')

    return [mid, ht, at, ref, kickoff, country, league, delete_time, live]


def get_fixture_list():
    # This function scrapes the fixtures due in the next 72 hours in the leagues covered in soccer_countries_leagues.
    # It saves the fixture info into a mysql table for later use by our form scraper.

    path = PATH_TO_DRIVER
    driver = webdriver.Chrome(service=Service(path))

    username = ?
    password = ?
    hostname = ?
    port = ?
    database = ?

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
        query = "INSERT INTO fixtures_dictionary" +
                "(id, home, away, referee, kickoff, country, league, delete_time, live)" + 
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, row)
    cursor.close()
    cnx.commit()

get_fixture_list()
