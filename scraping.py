import requests
from requests.exceptions import *
from requests.adapters import HTTPAdapter
import json
import pymongo
from pymongo.errors import *
import time

from concurrent.futures import ThreadPoolExecutor
from requests_futures.sessions import FuturesSession

import logging

from urllib3 import Retry

logging.basicConfig(filename="scrape.log", filemode="w", level=logging.INFO)

FORBIDDEN = 403

executor = ThreadPoolExecutor(max_workers=100)
session = requests.Session()
fsession = FuturesSession(executor)


def fetch_crawlers(filename="crawlers.txt"):
    crawlers = {}
    counter = 0
    with open("crawlers.txt", "r") as f:
        user = f.readline().strip()
        token = f.readline().strip()
        _ = f.readline().strip()
        while user != "":
            crawlers[counter] = {"user": user, "token": token}
            user = f.readline().strip()
            token = f.readline().strip()
            _ = f.readline()
            counter += 1
    return crawlers


def make_sess(n_attempts):
    adapter = HTTPAdapter(max_retries=Retry(total=n_attempts, status_forcelist=[403], backoff_factor=0.8))
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def make_sess_async(n_attempts):
    adapter = HTTPAdapter(max_retries=Retry(total=n_attempts, status_forcelist=[403], backoff_factor=0.8))
    fsession.mount("http://", adapter)
    fsession.mount("https://", adapter)
    return fsession


def attempt_request(url, n_attempts=10, auth=None, headers=None):
    sess = make_sess(n_attempts=n_attempts)
    logging.info("Requesting url %s" % url)

    try:
        r = sess.get(url, auth=auth, headers=headers)
        return r
    except RequestException as e:
        logging.error("Received the following exception while requesting the following url: %s \n %s" % (url, e))


def attempt_request_async(url, n_attempts=10, auth=None, headers=None):
    sess = make_sess_async(n_attempts)
    logging.info("Asynchronously requesting url: %s" % url)

    try:
        r = sess.get(url, auth=auth, headers=headers)
        return r
    except RequestException as e:
        logging.error("Received the following exception while asynchronously requesting the following url: %s \n"
                      " %s" % (url, e))


def get_orgs(since=0, api_url='https://api.github.com/organizations?since='):
    r = attempt_request(api_url + str(since))
    return r if r is None else json.loads(r.text)


def save_to_db(data, col,
               db="patents",
               db_url="mongodb://localhost:27017/admin?readPreference=primary&appname=MongoDB%20Compass&ssl=false"):
    myclient = pymongo.MongoClient(db_url)
    mydb = myclient[db]
    mycol = mydb[col]
    try:
        mycol.insert_one(data)
    except PyMongoError as e:
        if col == "organization":
            logging.error("Unable to save info for organisation %d, error %s: " % (data["id"], e))


def change_ip():
    logging.info("Trying to change IP...")
    max_attempts = 10
    attempts = 0
    while True:
        attempts += 1
        try:
            print('GETTING NEW IP')
            logging.info("GETTING NEW IP")
            print('SUCCESS')
            logging.info("SUCCESS")
            return
        except Exception as e:
            if attempts > max_attempts:
                logging.error('Max attempts reached for VPN. Check its configuration.')
                logging.error('Browse https://github.com/philipperemy/expressvpn-python.')
                logging.error('Program will exit.')
                exit(1)
            logging.error(e)
            logging.error('Skipping exception.')


def maybe_wait(response, be_nice=True):
    if "X-RateLimit-Reset" in response.headers and "X-RateLimit-Remaining" in response.headers:
        if int(response.headers["X-RateLimit-Remaining"]) == 0:
            if be_nice:
                wait_time = (float(response.headers["X-RateLimit-Reset"]) - time.time())
                logging.info("Waiting %lf seconds until rate limit reset" % wait_time)
                time.sleep(wait_time)
            else:
                change_ip()
                time.sleep(2)
        return True
    return False
