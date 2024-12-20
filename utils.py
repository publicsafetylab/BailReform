import os

from dotenv import load_dotenv
from multiprocessing.pool import ThreadPool
from pymongo import MongoClient
from tqdm import tqdm


class MongoCollections:
    def __init__(self):
        load_dotenv()
        self.client = MongoClient(os.getenv("MONGO_READ_URI"))
        self.jdi = self.client.get_database("jdi")
        self.jdi_stats = self.client.get_database("jdi-stats")
        self.bookings = self.jdi.get_collection("jdi")
        self.scrape_dates = self.jdi_stats.get_collection("scrape-dates")


def thread(worker, jobs, threads=5):
    pool = ThreadPool(threads)
    results = list()
    for result in tqdm(pool.imap_unordered(worker, jobs), total=len(jobs)):
        if result and isinstance(result, list):
            results.extend([r for r in result if r])
        elif result:
            results.append(result)
    pool.close()
    pool.join()
    if results:
        return results
    