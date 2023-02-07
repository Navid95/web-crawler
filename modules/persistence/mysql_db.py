from mysql.connector import Error
import mysql.connector
import modules.persistence.mysql_alchemy_core as alchemy
import logging


FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(filename=f'../../logs/db-mysql-log.txt', encoding='utf-8', level=logging.INFO, format=FORMAT)

#  Const Variables
data_base_name = 'web_crawler'
table_name = 'urls'
relation_table_name = 'parent_child'
create_db = f''' create table if not exists urls (
                id int auto_increment,
                url text not null,
                primary key (id)
                ) ENGINE = InnoDB'''
drop_table = f'''drop table {table_name}'''

"""
initiates mysql db connection with the provided details: str(host ip/domain name), str(username), 
str(pass phrase), optional str(data base name)
returns connection object on success
"""


def init_connection(host, username, passwd, db_name=None):
    try:
        connection = mysql.connector.connect(host='localhost', user='crawler', passwd='1qaz!QAZ')
        logging.info(f'Successfully connected to mysql database: {username}@{host}')
        connection.cmd_init_db(db_name) if db_name != None else logging.INFO(
            f'no database name provided for the connection')
        return connection
    except Error as e:
        logging.exception(f'Exception in connecting to mysql database: {username}@{host}')
        return None


"""
receives a connection objects and closes it: Connection(connection)
returns: None
"""


def close(connection):
    try:
        connection.close()
        logging.info(f'Successfully closed mysql db connection: {connection.connection_id}')

    except Error as e:
        logging.exception(f'Exception in closing the mysql db connection: {connection.connection_id}')


"""
gets a connection object and a query and executes the query on connection: Connection(connection), str(query)
returns: None
"""


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info(f'Query executed successfully: "{query}"')
        connection.commit()
    except Error as e:
        logging.exception(f'Exception in executing the query: "{query}"')


"""
gets a connection object and an insert query and executes the query on connection: Connection(connection), str(query)
acts as an interface for better maintenance
returns: calls one of the insert implementation functions
"""


def insert(connection, values):
    return __insert_v3__(connection, values)


"""
gets a connection object and a list of values and executes the insert query on connection: Connection(connection), str(query)
uses execute_query under the hood with a string query created from the values received
returns: None
"""


def __insert_v1__(connection, values):
    items = prepare_iterable_query(values)
    if items is not None:
        query = f'''insert into {table_name} values{items.__str__().replace("[","").replace("]","")}'''
        execute_query(connection, query)
        logging.info(f'inserting values into {table_name}. count:{len(items)}')
    else:
        logging.error(f'Error in preparing the insert values')


"""
gets a connection object and a list of values and executes the insert query on connection: Connection(connection), str(query)
uses built-in executemany(query, values)
returns: None
"""


def __insert_v2__(connection, values):
    query = f'''INSERT INTO {table_name} (id,url) VALUES ( %s, %s )'''
    items = prepare_iterable_query(values)
    try:
        cursor = connection.cursor()
        cursor.executemany(query, items)
        connection.commit()
        logging.info(f'Query executed successfully: "{query}"')
    except Error as e:
        logging.exception(f'Exception in executing the query: "{query}"')


"""
gets a connection object and a list of values and executes the insert query on connection: Connection(connection), str(query)
uses execute_query under the hood with a string query created from the values received
NOTE: inserts take place one by one
returns: None
"""


def __insert_v3__(connection, values):
    items = prepare_iterable_query(values)
    if items is not None:
        for i in items:
            query = f'insert into {table_name} values {i.__str__().replace("[","").replace("]","")}'
            execute_query(connection, query)
            logging.info(f'inserting value {i} into {table_name}')
    else:
        logging.error(f'Error in preparing the insert values')


"""
gets an iterable containing values and creates a list containing tuples: Iterable(values)
returns: list[Tuple1(), Tuple2(), ...]; None if type of received values is not list
"""


def prepare_iterable_query(values):
    logging.info(f'received values to change to tuple. count:{len(values)}')
    logging.info(f'received values to change to tuple. values:{values}')
    if type(values) == list:
        logging.info(f'values are in list type. converting to tuple')
        tuple_values = list()
        for x in values:
            tuple_values.append(tuple((0, x)))
        logging.info(f'converted list values to a list of tuples: {tuple_values}')
        return tuple_values
    else:
        return None


def save_url(seed, children):
    alchemy.save_url(seed, children)


def query_url(seed):
    return alchemy.query(seed)


def blacklist_url(url):
    alchemy.blacklist_url(url)


def get_blacklist():
    return alchemy.get_blacklist()

