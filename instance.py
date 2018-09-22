from multiprocessing import Process
import requests
from bs4 import BeautifulSoup
from urllib3.util import parse_url
from urllib.parse import urljoin
import tldextract
import time


class Crawler(Process):

    def __init__(self, _pid, _queue, _urls, _wait=1, _parser='lxml'):
        super().__init__()
        self.id = _pid
        self.q = _queue
        self.urls = _urls
        self.wait_time = _wait
        self.parser = _parser
        self.disallowed = ['instagram',
                           'facebook',
                           'twitter',
                           't',
                           'wikipedia',
                           'youtube',
                           'pinterest',
                           'google',
                           'youtu',
                           'reddit']

    @staticmethod
    def extract_root(url):
        url = parse_url(url)
        url = url.scheme + '://' + url.host
        return url

    def parse_url_list(self, urls):
        for url in urls[:]:
            if url.startswith('mailto:'):
                urls.remove(url)

            if tldextract.extract(url).domain in self.disallowed:
                urls.remove(url)
        return urls

    def parse_url(self, link, url):
        link = str(link)
        url = str(url)
        if tldextract.extract(url).domain in self.disallowed:
            return ''
        return urljoin(url, link)

    def run(self):
        # begin crawling 0 depth and send
        # (urls, html) for distribution on main process
        links_found = 0
        for url in self.urls:
            # TODO: add robots.txt handling
            # for now lets wait x amount of time before browsing the next url as a general 'catch-all'
            try:
                if url is '' or url is None or url is 'None' or 'javascript:' in url:
                    continue
                time.sleep(self.wait_time)
                response = requests.get(url)
                html = response.content
                soup = BeautifulSoup(html, self.parser)

                links = [link.get('href') for link in soup.find_all('a')]
                links = [self.parse_url(link, url) for link in links]
                links = self.parse_url_list(links)
                links = [link for link in links if link]
                links_found += len(links)
                # queue ended up not being big enough for 50+ urls per iteration * 20 threads
                self.q.put({'urls': links,
                            'html': html,
                            'crawled_url': url,
                            'time': int(time.time()),
                            'linkbacks': 0})

            except Exception as e:
                print(str(e))

        print(f'Worker {self.id} finished crawling {len(self.urls)} urls and found {links_found} links')
