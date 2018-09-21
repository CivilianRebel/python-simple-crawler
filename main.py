import multiprocessing
from instance import Crawler
from pymongo import MongoClient
import time
import numpy as np


class Boss:

    def __init__(self, _threadcount, batch_size):
        self.CPU_SIZE = _threadcount
        self.urls_per_batch = batch_size
        self.queue = multiprocessing.Manager().Queue()

        client = MongoClient()
        self.url_db = client.maindb
        self.rawdata_db = client.maindb.raw_html

        multiprocessing.freeze_support()
        self.start_url = 'https://www.w3schools.com/python/python_mongodb_delete.asp'
        self.bootstrap()
        self.run()

    def bootstrap(self):
        init_proc = Crawler(-1, self.queue, [self.start_url])
        init_proc.start()

        init_proc.join()
        result = (['null'], 'null')
        # print(queue.get())
        while not self.queue.empty():
            result = dict(self.queue.get())
        urls = result['urls']
        html = result['html']
        del result
        del init_proc
        if html is 'null':
            raise AttributeError("Didn't get any urls from initial seed...")
        [self.url_db.unfetched.insert_one({'url': url}) for url in urls]

    def fresh_batch(self):
        i = 0
        urls = []
        for link in self.url_db.unfetched.find():
            if i <= self.urls_per_batch:
                self.url_db.unfetched.delete_one({'url': link['url']})
                urls.append(link['url'])
                # print(link['url'])
            else:
                break
            i += 1
        urls = np.array_split(urls, self.CPU_SIZE)
        return urls, i

    def spawn(self, urls):
        i = 0
        w = []
        for n in urls:
            worker = Crawler(_pid=i, _queue=self.queue, _urls=n)
            worker.start()
            # print(f'worker {i} started')
            w.append(worker)
            i += 1
        print(f'{i+1} workers started')
        return w

    def handle_results(self):
        results = []
        while not self.queue.empty():
            results.append(dict(self.queue.get()))
        t = 0
        for r in results:
            insert = {'url': r['crawled_url'],
                      'html': r['html'],
                      'time': int(time.time())
                      }
            self.rawdata_db.insert_one(insert)
            insert = {'url': r['crawled_url'], 'time': int(time.time())}
            self.url_db.fetched.insert_one(insert)

            for url in r['urls']:
                insert = {'url': url}
                query = {'url': url}
                if not self.url_db.fetched.find_one(query):
                    t += 1
                    self.url_db.unfetched.insert_one(insert)
        return t

    def run(self):
        while True:
            urls, count = self.fresh_batch()

            print(f'Fetching {count} urls across {len(urls)+1} threads')

            workers = self.spawn(urls)

            [w.join() for w in workers]

            total_urls = self.handle_results()

            print(f'Found total of {total_urls} urls this round, looping process now')


if __name__ == '__main__':
    b = Boss(12, 200)
