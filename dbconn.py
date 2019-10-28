#!/usr/bin/python
# http://www.postgresqltutorial.com/postgresql-python/connect/
import psycopg2
import psycopg2.extras
import config as cfg

_author_ = "Colin Sippl"
_organization_ = "University Library of Regensburg"
_email_ = "colin.sippl@ur.de"
_license_ = "GPL 3"


class Dbconn:

    def __init__(self):
        self.cur = None
        self.conn = None
        self.result = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            # params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            # conn = psycopg2.connect(**params)
            self.conn = psycopg2.connect(
                "dbname=" + cfg.DB_NAME + " user=" + cfg.DB_USER + " host=" + cfg.DB_HOST + " password=" + cfg.DB_PW)
            # conn = psycopg2.connect("dbname=" + cfg.DB_NAME + " user=" + cfg.DB_USER + " password=" + cfg.DB_PW)
            # sd
            # create a cursor
            self.cur = self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

            # execute a statement
            # print('PostgreSQL database version:')
            # self.cur.execute('SELECT version()')
            # self.cur.execute('SELECT * FROM vl_people')

            # display the PostgreSQL database server version
            # db_version = self.cur.fetchone()
            # print(db_version)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def execute_st(self, statement):
        """ Execute SQL satement """
        try:
            # statement = 'SELECT * FROM vl_people'
            # execute a statement
            print('PostgreSQL database version:')
            # self.cur.execute('SELECT version()')
            self.cur.execute(statement)

            # display the PostgreSQL database server version
            self.result = self.cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def fetch_result(self):
        return self.result

    def disconnect(self):
        """ Disconnect from the PostgreSQL database server """
        # close the communication with the PostgreSQL

        self.cur.close()
        # except (Exception, psycopg2.DatabaseError) as error:
        #    print(error)
        # finally:
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    dbconn = Dbconn()
    dbconn.connect()
    dbconn.execute_st('SELECT * FROM vl_people LIMIT 10')
    dbconn.fetch_result()
    dbconn.disconnect()