import logging
from modules.crawler import crawler as crw
from modules.persistence import mysql_db
from modules.persistence import persistence as save

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(filename=f'./logs/log-1.txt', encoding='utf-8', level=logging.INFO, format=FORMAT)


# TODO unit tests!!!

def init():
    seedtuple = list()
    seedtuple.append(tuple(('https://us.cnn.com/', ['https://us.cnn.com/'])))
    # seedtuple.append(tuple(('https://www.facebook.com/', ['https://www.facebook.com/'])))
    # seedtuple.append(tuple(('https://www.instagram.com/', ['https://www.instagram.com/'])))
    # seedtuple.append(tuple(('https://twitter.com/?lang=en', ['https://twitter.com/?lang=en'])))
    result = crw.crawl(seedtuple, depth=2)
    save.persist_crawl_links_file(result)
    insert_values = list()
    for key in result.keys():
        insert_values.append(key)
    for val in result.values():
        insert_values.extend(val)
    mysql_db.save_url(seedtuple[0][0], insert_values)
    for child in mysql_db.query_url('https://www.nytimes.com/'):
        print(child)
    exit(0)


init()
