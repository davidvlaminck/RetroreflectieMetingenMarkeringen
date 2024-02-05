from pathlib import Path

from psycopg2.pool import ThreadedConnectionPool


class PostGISConnector:
    def __init__(self, host: str, port: str, user: str, password: str, database: str):
        self.pool = ThreadedConnectionPool(minconn=5, maxconn=20, user=user, password=password, host=host, port=port,
                                           database=database)
        self.main_connection = self.pool.getconn()
        self.main_connection.autocommit = False
        self.db = database

    def set_up_tables(self, file_path=Path('sql_files/create_tables.sql')):
        with self.main_connection.cursor() as cursor:
            with open(file_path) as setup_queries:
                queries = setup_queries.readlines()
                query = ' '.join(queries)
                cursor.execute(query)
                self.main_connection.commit()

    def execute_query(self, query: str):
        with self.main_connection.cursor() as cursor:
            cursor.execute(query)
            self.main_connection.commit()

    def get_connection(self):
        connection = self.pool.getconn()
        connection.autocommit = False
        return connection

    def kill_connection(self, connection):
        self.pool.putconn(connection)

    def create_additional_tables_by_year(self, report_year: int,
                                         file_path=Path('sql_files/create_tables_year_specific.sql')):
        with self.main_connection.cursor() as cursor:
            with open(file_path) as setup_queries:
                queries = setup_queries.readlines()
                query = ' '.join(queries)
                query = query.replace('YYYY', str(report_year))
                cursor.execute(query)
                self.main_connection.commit()
