import requests
from bs4 import BeautifulSoup
import instance
from urllib3.util import parse_url
# didnt work so i used numpy instead
# deprecated
def split(arr, n_chunks):
    for i in range(0, len(arr), n_chunks):
        yield arr[i:i + n_chunks]


c = instance.Crawler(1, 10, True)
url = 'https://slate.com/culture/2018/09/tiger-woods-2018-bmw-championship.html'
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html, c.parser)
links = [link.get('href') for link in soup.find_all('a')]
links = [c.parse_urls(link, url) for link in links]
[print(parse_url(link).host, '\n') for link in links]
