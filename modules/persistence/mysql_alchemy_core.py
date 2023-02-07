import logging
import sqlalchemy as alchemy
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text, MetaData, Table, Column, Integer, String, ForeignKey, insert, select, bindparam, \
    literal_column, and_, or_, func

FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(filename=f'../../logs/mysql-alchemy-core-log.txt', encoding='utf-8', level=logging.INFO,
                    format=FORMAT)

#  TODO change DB credentials
connection_string = 'mysql+pymysql://root:!QAZ1qaz@localhost:3306/web_crawler'
engine = alchemy.create_engine(connection_string)
meta_obj = MetaData()
urls_table = Table('urls', meta_obj,
                   Column(name='id', type_=Integer, autoincrement=True, primary_key=True),
                   Column(name='url', type_=String(250), unique=True)
                   )

parent_child_table = Table('parent_child', meta_obj,
                           Column(name='id', type_=Integer, autoincrement=True, primary_key=True),
                           Column('parent_id', ForeignKey('urls.id'), nullable=False),
                           Column('child_id', ForeignKey('urls.id'), nullable=False)
                           )

blacklist_table = Table('blacklist', meta_obj,
                        Column(name='id', type_=Integer, autoincrement=True, primary_key=True),
                        Column('domain', type_=String(50), nullable=False, unique=True))

meta_obj.create_all(engine)


def save_url(seed, children):
    urls = list()
    results = list()
    child_inserts = list()
    insert_stm = insert(urls_table).values(url=seed)
    urls.append(insert_stm)

    for url in children:
        urls.append(insert(urls_table).values(url=url))
    with engine.connect() as conn:
        for i in urls:
            try:
                results.append(conn.execute(i))
            except IntegrityError as e:
                logging.exception(e)
                continue
            except BaseException as e:
                logging.exception(e)
                continue
        conn.commit()
        for res in results:
            # print(res.inserted_primary_key[0])
            insert_stm = insert(parent_child_table).values(parent_id=results[0].inserted_primary_key[0],
                                                           child_id=res.inserted_primary_key[0])
            try:
                child_inserts.append(conn.execute(insert_stm))
            except BaseException as e:
                logging.exception(e)
                continue
        conn.commit()


def query(seed):
    # print(f'quering {seed} on database')

    sub_query = select(urls_table.c.id).where(urls_table.c.url == seed).scalar_subquery()

    select_stm = select(urls_table.c.url) \
        .join_from(urls_table, parent_child_table, urls_table.c.id == parent_child_table.c.child_id) \
        .where(parent_child_table.c.parent_id == sub_query)
    # print(select_stm)

    with engine.connect() as conn:
        result = conn.execute(select_stm)
    return [child.url for child in result]


def get_url_id(url):
    logging.info(f'quering to find id value of {url}')
    select_stm = select(urls_table).where(urls_table.c.url == url)
    with engine.connect() as conn:
        result = conn.execute(select_stm)
    logging.info(f'returning {result[0].id} as id of {url}')
    return result[0].id


def blacklist_url(url):
    logging.info(f'adding {url} to blacklist table')
    # url_id = select(urls_table).where(urls_table.c.url == url).scalar_subquery()
    insert_stm = insert(blacklist_table).values(domain=url)
    logging.debug(f'Blacklist query: {insert_stm}')
    try:
        with engine.connect() as conn:
            result = conn.execute(insert_stm)
            conn.commit()
            logging.info(f'inserted {url} to blacklist table')
    except BaseException as e:
        logging.exception(e)


"""
fetches the blacklist domains from DB
returns: list(url)
"""


def get_blacklist():
    logging.info(f'quering all blacklisted urls from DB')
    select_stm = select(blacklist_table)
    try:
        with engine.connect() as conn:
            result = conn.execute(select_stm)
        return [item.domain for item in result]
    except BaseException as e:
        logging.exception(e)
        return None
