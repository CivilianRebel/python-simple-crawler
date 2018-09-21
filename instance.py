from multiprocessing import Process
import requests
from bs4 import BeautifulSoup
from urllib3.util import parse_url
import time


class Crawler(Process):

    def __init__(self, _pid, _queue, _urls, _wait=1, _parser='lxml'):
        super().__init__()
        self.id = _pid
        self.q = _queue
        self.urls = _urls
        self.wait_time = _wait
        self.parser = _parser

    @staticmethod
    def extract_root(url):
        url = parse_url(url)
        url = url.scheme + '://' + url.host
        return url

    def parse_urls(self, link, url):
        link = str(link)
        url = str(url)
        parsed = parse_url(url)
        if link == '#':
            return parsed.scheme + '://' + parsed.host + '/' + parsed.path
        elif link.startswith('/'):
            return self.extract_root(url) + link
        else:
            return parsed.scheme + '://' + parsed.host + '/' + parsed.path

    def run(self):
        # begin crawling 0 depth and send
        # (urls, html) for distribution on main process
        links_found = 0
        for url in self.urls:
            # TODO: add robots.txt handling
            # for now lets wait x amount of time before browsing the next url as a general 'catch-all'
            time.sleep(self.wait_time)
            response = requests.get(url)
            html = response.content
            soup = BeautifulSoup(html, self.parser)
            links = [link.get('href') for link in soup.find_all('a')]
            links = list(set([self.parse_urls(link, url) for link in links]))
            links_found += len(links)
            self.q.put({'urls': links, 'html': html, 'crawled_url': url})
        print(f'Worker {self.id} finished crawling {len(self.urls)} urls and found {links_found} links')
