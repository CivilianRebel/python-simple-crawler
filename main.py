import multiprocessing
from instance import Crawler
from pymongo import MongoClient, UpdateOne
import time
import numpy as np
from tqdm import tqdm


class Boss:

    def __init__(self, _threadcount, batch_size, init=False):
        self.CPU_SIZE = _threadcount
        self.urls_per_batch = batch_size
        self.queue = multiprocessing.Manager().Queue()

        client = MongoClient()
        self.url_db = client.maindb
        self.rawdata_db = client.maindb.raw_html

        multiprocessing.freeze_support()
        self.start_url = 'https://www.gq.com/story/football-and-my-dads-dementia'
        if init:
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
        urls = []
        i = 1
        for link in self.url_db.unfetched.find().limit(self.CPU_SIZE * self.urls_per_batch):
            self.url_db.unfetched.delete_one({'url': link['url']})
            urls.append(link['url'])
            # print(link['url'])
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
        print(f'{i} workers started')
        return w

    def handle_results(self):
        fetched = []
        unfetched = []
        while not self.queue.empty():
            temp = dict(self.queue.get())
            # create list of dictionaries for bulk insert operation
            fetched.append({'url': temp['crawled_url'],
                            'raw': temp['html'],
                            'linkbacks': temp['linkbacks'],
                            'time': temp['time']})
            # create 1-D list of unfetched urls
            for url in temp['urls']:
                unfetched.append(url)

        self.rawdata_db.insert_many(fetched)
        print(f'Successfully inserted {len(fetched)} fetched urls')
        print('Iterating over unfetched now...')
        time.sleep(0.1)
        start = time.time()
        unfetched = list(set(unfetched))

        existing_urls = list()
        linkbacks = list()
        inserts = []
        updates = []
        t = 0
        # get all values that match the links we found
        for r in self.rawdata_db.find({'url': {'$in': unfetched}}):
            existing_urls.append(r['url'])
            linkbacks.append([r['_id'], r['linkbacks']])
        # remove the fetched links from unfetched
        unfetched = [item for item in unfetched if item not in existing_urls]
        # create the insert transaction
        for url in unfetched:
            t += 1
            inserts.append({'url': url})
        # create the update_linkback transaction
        for value in linkbacks:
            uid = value[0]
            linkback_new = int(value[1]) + 1
            updates.append(UpdateOne({'_id': uid}, {'$set': {'linkbacks': linkback_new}}))

        # bulk update and insert database to increase throughput
        self.url_db.unfetched.insert_many(inserts)
        del inserts  # probably useless lol
        self.rawdata_db.bulk_write(updates)
        del updates

        print(f'Total time {str(float(time.time()-start))[:3]} for {len(unfetched)} unfetched urls')

        time.sleep(0.1)
        return t

    def run(self):
        while True:
            urls, count = self.fresh_batch()

            # print(f'Fetching {count} urls across {len(urls)+1} threads')

            workers = self.spawn(urls)

            [w.join() for w in workers]

            total_urls = self.handle_results()

            print(f'Found total of {total_urls} urls this round, looping process now')


if __name__ == '__main__':
    b = Boss(12, 10, init=True)
