import logging


FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(filename=f'../../logs/persistence-log.txt', encoding='utf-8', level=logging.INFO, format=FORMAT)

"""
gets a dictionary of seeds:[child urls] and persists them to mysql DB
returns: None
"""


def persist_db_mysql(seed_dic):
    for seed in seed_dic.keys():
        pass


def persist_crawl_links_file(link_dict):
    try:
        for key in link_dict:
            file = open(f'resources/{key.replace("https://","").replace("www", "").replace("/","_")}', 'w+')
            file.writelines(url + '\n' for url in link_dict[key])
            file.close()
    except BaseException as e:
        logging.exception(f'Error on persisting data to file')


def persist_response(list):
    try:
        for l in list:
            logging.info(f'persisting {l.url} contents')
            file = open(f'resources/{l.url.replace("https://","").replace("/","_")}' , 'w+')
            file.write(l.text)
            file.close()
    except BaseException as e:
        logging.exception(f'Error on persisting data to file')

