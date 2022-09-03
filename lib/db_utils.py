from lib.constants import SQL_DIR
import sqlite3


def initialize_tables(path_to_db):
    """Init the tables of our DB, setting primary keys.
    Need to do this up front with SQLite, cannot ALTER to add primary keys later.
    """

    connection = sqlite3.connect(path_to_db)

    with open(SQL_DIR/'init.sql') as f:
        query = f.read()

    with connection:
        connection.executescript(query)
        connection.commit()

