from bs4 import BeautifulSoup
from multiprocessing import Pool, Queue
from datetime import datetime
import tld
import sqlite3
import sys
from tld.utils import update_tld_names
import time
import os
import requests

update_tld_names()
e, w, l = 0, 1, 2
start_url = 'https://pythonprogramming.net/'
banned = ['facebook.com', 'twitter.com', 't.co', 'instagram.com']


def log(string, type):
    string = str(string)
    ts = time.time()
    t = str(datetime.fromtimestamp(ts).strftime('[%H:%M:%S] '))
    prefix = ''
    if type is e:
        prefix = 'ERROR: ' + t
    elif type is w:
        prefix = 'WARN: ' + t
    elif type is l:
        prefix = 'LOG: ' + t

    exists = os.path.isfile('logs.txt')

    if not exists:
        with open('logs.txt', 'w') as f:
            f.write('NEW LOG \n \n')

    with open('logs.txt', 'a') as log:
        log.write(str(prefix + string) + '\n')


def handle_local_links(url, link):
    if link.startswith('/'):
        return ''.join([url, link])
    else:
        return link


# add crawled urls to file
def completed(url, c):
    try:
        result = c.execute(f"INSERT INTO urls_visited (url, timestamp) VALUES ('{url}', '{int(time.time())}')")
        c.commit()
    except FileNotFoundError as er:
        log(str(er)+'\n\n', 0)
    except Exception as er:
        log(str(er) + '\n\n', 0)


def check_allowed(link):
    link = tld.get_fld(link)
    if link.lower() in banned:
        return False
    return True


def check_url_visited(url, c):
    result = c.execute(f'SELECT count(*) FROM urls_visited WHERE url="{url}"')
    result = result.fetchall()
    result = True if result[0][0] is 1 else False
    return result


def get_links(q):
    connection = sqlite3.connect('main.db')
    # print(os.getpid(), 'Successfully connected')
    while True:
        url = q.get(True)
        # test if url has been visited
        # returns true if url has been visited
        if check_url_visited(url, connection):
            url = q.get(True)
        try:
            r = requests.get(str(url))
            soup = BeautifulSoup(r.text, 'lxml')
            links = [link.get('href') for link in soup.find_all('a')]
            links = [handle_local_links(url, link) for link in links]
            links = list(set(links))
            for link in links:
                if check_allowed(link):
                    q.put(link)
            com = f"INSERT INTO crawler_data (raw, timestamp) VALUES ('{r.text}', '{int(time.time())}')"
            connection.execute(com)
            connection.commit()
            completed(url, connection)
        except TypeError as er:
            log(er, e)
            # print('Logged TypeError')
        except IndexError as er:
            log(er, e)
            # print('Logged IndexError')
        except AttributeError as er:
            log(er, e)
            # print('Logged AttributeError')
        except Exception as er:
            log(str(er), e)
            # print('Logged UNKNOWN ERROR CHECK LOG IMMEDIATELY')


def printout(q):
    while True:
        sys.stdout.write(f'\r{q.qsize()}')


def main():
    if os.path.isfile('logs.txt'):
        os.remove('logs.txt')

    q = Queue()
    p = Pool(12, get_links, (q,))
    q.put(start_url)
    new_p = Pool(1, printout, (q, ))
    while True:
        a1 = 1
        # q.put(str(input('\nEnter url to add: ')))

if __name__ == '__main__':
    main()
