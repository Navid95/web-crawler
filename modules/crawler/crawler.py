from bs4 import BeautifulSoup
from requests import Session, exceptions
from requests.exceptions import ConnectTimeout
import logging
from modules.persistence import mysql_db as db
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(filename=f'../logs/crawler-log-22.txt', encoding='utf-8', level=logging.INFO, format=FORMAT)

visited = set()
blacklist_db = db.get_blacklist()
logging.info(f'Loaded blacklist from DB: {blacklist_db}')
print(f'Loaded blacklist from DB: {blacklist_db}')
blacklist = set()
blacklist.update(blacklist_db) if blacklist_db is not None else None


"""
1- get seed html page ---> tuple(base, page)
2- extract child links
3- repeat (1) for child links
4- if desired depth is reached: return dict{base:[list of child links]}
"""


def crawl(seeds, depth=1):
    seed_links = dict()
    for base, url in seeds:
        logging.info(f'sending {url} to crawler module')
        seed_links[base] = crawl_depth(url, depth)
    return seed_links


"""
gets the list of child links and crawls them recursively: [links]
returns list of links: [links]
"""


def crawl_depth(links, depth=1):
    logging.info(f'crawler module received links to crawl. Depth remained: {depth}')
    child_links = list()
    url_pages = list()
    if depth > 0:
        session = Session()
        # get seed html page
        for url in links:
            if allow_visit(url):
                logging.info(f'crawling new url: {url}')
                try:
                    visited.add(url)
                    page = session.get(url, timeout=10)
                except (MaxRetryError, ConnectTimeoutError,ConnectTimeout, TimeoutError, ConnectionError, ConnectionAbortedError, ConnectionRefusedError, ConnectionResetError) as e:
                    logging.exception(f'Exception in get request to {url}')
                    logging.exception(f'Should add url:{url} to blacklist')
                    add_to_blacklist(url)
                    continue
                except BaseException as e:
                    logging.exception(f'Exception in get request to {url}\n{e}')
                    add_to_blacklist(url)
                    continue
                if page.status_code == 200:
                    logging.info(f'get request successful. {page.url}:{page.status_code}')
                    url_pages.append(tuple((url, page)))
                else:
                    logging.warning(f'get request got failed. {page.url}:{page.status_code}')
            else:
                logging.info(f'already visited {url}, skipping this url')
                continue
        session.close()
        for url, html in url_pages:
            child_links.extend(get_child_links(tuple((url, html))))
        depth = depth - 1
        return crawl_depth(child_links, depth)
    else:
        return links


""" 
gets a url an determines if it should be visited or not (already visited): str(url) 
returns boolean: allow(True) OR forbid(False)
"""


def allow_visit(url):
    return True if url not in visited and not is_blacklisted(mediate(url)) else False


"""
gets a url and changes it to the domain name to use in black listing: str(url)
returns: str(domain-name)
"""


def mediate(url):
    root = str
    url = url.replace('www.', '').replace('http://', '').replace('https://', '')
    domain = url.split('/')[0]
    return domain


# TODO add to visited function
def add_to_visited():
    pass


"""
gets a url and checks if it is black listed or not: str(url)
returns: boolean: blacklisted(True) OR not blacklisted(False)
"""


def is_blacklisted(url):
    return True if url in blacklist else False


"""
adds a mediated url to the blacklist: str(url)
returns: None
"""


def add_to_blacklist(url):
    mediated = mediate(url)
    logging.info(f'adding mediated url {mediated} to blacklist')
    blacklist.add(mediated)
    db.blacklist_url(mediated)


"""
gets page_tuple: (baseURL, htmlPage) 
returns list of page's child links: [links] 
"""


def get_child_links(page_tuple):
    return construct_links(extract_raw_links_string(page_tuple))


"""
gets raw links tuple: (baseURL, [raw links])
returns: list of complete urls: [complete urls]
"""


def construct_links(page_links_tuple):
    logging.info(f'extracting complete links from raw links')
    children = []
    for l in page_links_tuple[1]:
        if l.startswith('http') or l.startswith('https'):
            children.append(l)
        #     Relative links construction
        elif l.startswith('/'):
            logging.info(f'received relative link {l} from base URL {page_links_tuple[0]}')
            children.append(page_links_tuple[0] + l) if page_links_tuple[0][-1] != '/' else children.append(
                page_links_tuple[0] + l[1:])
            logging.info(f'appended constructed link {children[-1]} to children list')
    return children


"""
gets page tuple and extracts all <a href='blah blah blah'></a> tags 
with href attribute: str(html page content): (baseURL, htmlPage)
returns: tuple of baseURL and <a> tag bodies: (baseURL, [str(body of a tag), ...])
"""


def extract_raw_links_string(page_tuple):
    logging.info(f'looking for "a" tags with "href" attribute in html string')
    html = page_tuple[1]
    links = list()
    document = BeautifulSoup(html.text, 'html.parser')
    tags = document.find_all('a', recursive=True)
    for t in tags:
        if t.has_attr('href'):
            links.append(t['href'])
    return tuple((page_tuple[0], links))
