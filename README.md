# python-simple-crawler
Simple web crawler made in Python using BS4 and SQLite3

## Requirements
- BeautifulSoup
  - `pip install --upgrade bs4`
  - What better parser is out there anyways??
- TLD
  - `pip install --upgrade tld`
  - For easy domain extraction, and laziness
- (optional) LXML
  - `pip install --upgrade lxml`
  - Good and fast parser. If you end up using something else change the
    line in the `find_links` function that defines BeautifulSoup to
    match your parser.

