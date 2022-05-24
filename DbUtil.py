from configparser import ConfigParser
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor


class RDBMSUtil:
    def __init__(self):
        """
            Initializer
        """
        try:
            print ("hello from init")
            config_path = 'conf/db.conf'

            config = ConfigParser()
            config.read(config_path)

            database = config.get('Database', 'database')
            password = config.get('Database', 'password')
            port = config.get('Database', 'port')
            host = config.get('Database', 'host')
            user = config.get('Database', 'user')

            self.db_init_flag = 0
            para={'database':database,'user':user,'password':password,'host':host,'port':port}
            self.db = psycopg2.connect(**para)
        #    self.db = psycopg2.connect(database,user,password,host, port)
            print("Connection established successfully")
            # log.info("Connection established successfully")
        except Exception as e:
            # log.error(str(e), exc_info=True)
            raise Exception(str(e))
           # raise Exception("Exception while establishing connection to database")

    def check_table_exists(self, table):
        cursor = self.db.cursor()
        try:
            query = """SELECT 1 FROM information_schema.tables
            WHERE table_name = '{table_name}';""".format(table_name=table)
            cursor.execute(query)
            print(cursor.rowcount)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            # log.info("Error while checking the table")
        finally:
            cursor.close()

    def drop_table(self, table):
        """
            This function is to drop a table, if exists
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            qry = """DROP TABLE IF EXISTS """ + table + """;"""
            cursor.execute(qry)
            self.db.commit()
            # log.debug("Dropping Table was Successfull")
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Dropping Table - {}".format(str(e)))
            raise Exception("Error while dropping table")
        finally:
            cursor.close()

    def fetch_all_records(self, table):
        """
            This function is to fetch all records in a table
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            qry = """SELECT * FROM """ + table + """;"""
            cursor.execute(qry)
            rows = cursor.fetchall()
            # log.debug("Fetching all records was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Fetching Records - {}".format(str(e)))
            raise Exception("Error while Fetching Records")
        finally:
            cursor.close()

    def insert_record_by_json(self, table, input_json):
        """
            This function is to insert a record to a table
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            columns_list = []
            values_list = []
            for each_obj in input_json:
                column_name = each_obj.get("column", "")
                value = each_obj.get("value", "")
                columns_list.append(column_name)
                values_list.append(value)
            columns = columns_list
            records_list_template = ','.join(['%s'] * len(values_list))
            qry = '''INSERT INTO {table} ({columns}) VALUES ({values})'''.format(
                columns=', '.join(map(str, columns)), table=table, values=records_list_template)
            qry = qry.replace("'NULL'", "NULL")
            cursor.execute(qry, values_list)
            self.db.commit()
            # log.debug("Insertion of record is successful")
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Inserting Record - {}".format(str(e)))
            raise Exception("Error while Inserting the record")
        finally:
            cursor.close()

    def insert_multiple_record(self, table, input_json):
        """
            This function is to insert multiple record to a table
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            columns_list = []
            values_list = []
            for each_obj in input_json:
                values = []
                columns = []
                for each_item in each_obj:
                    column_name = each_item.get("column", "")
                    columns.append(column_name)
                    value = each_item.get("value", "")
                    values.append(value)
                columns_list.append(tuple(columns))
                values_list.append(tuple(values))

            columns = columns_list[0]
            records_list_template = ','.join(['%s'] * len(values_list))
            qry = '''INSERT INTO {table} ({columns}) VALUES {values}'''.format(columns=', '.join(map(str, columns)),
                                                                               table=table,
                                                                               values=records_list_template)
            qry = qry.replace("'NULL'", "NULL")
            cursor.execute(qry, values_list)
            self.db.commit()
            # log.debug("Insertion of records is successful")
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Inserting Records - {}".format(str(e)))
            raise Exception("Error while Inserting Records")
        finally:
            cursor.close()

    def update_table(self, table, values_condition, update_condition):
        """
            This function is to update a record in a table
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            qry = '''UPDATE {table} SET {values_condition} WHERE {update_condition}'''.format(
                table=table, values_condition=values_condition, update_condition=update_condition)
            cursor.execute(qry)
            rowcount = cursor.rowcount
            self.db.commit()
            # log.debug("Update table is successful")
            return rowcount
        except Exception as e:
            self.db.rollback()
            # log.exception("Error updating table - {}".format(str(e)))
            raise Exception("Error while updating table")
        finally:
            cursor.close()

    def fetch_all_records_with_condition(self, table, condition):
        """
            This function is to fetch all records with a condition
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            qry = """SELECT * FROM """ + table + """ WHERE """ + condition + """;"""
            cursor.execute(qry)
            rows = cursor.fetchall()
            # log.debug("Fetching all records with condition was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Fetching Records - {}".format(str(e)))
            raise Exception("Error while fetching records")
        finally:
            cursor.close()

    def fetch_specified_columns_with_condition(self, table, columns_list, condition):
        """
            This function is to fetch specified columns with a condition
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            columns = tuple(columns_list)
            qry = '''SELECT {columns} FROM {table} WHERE {condition}'''.format(columns=', '.join(map(str, columns)),
                                                                               table=table, condition=condition)
            cursor.execute(qry)
            rows = cursor.fetchall()
            # log.debug("Fetching specific columns with condition was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Fetching Records - {}".format(str(e)))
            raise Exception("Error while fetching records")
        finally:
            cursor.close()

    def fetch_specified_columns(self, table, columns_list):
        """
            This function is to fetch specified columns with out any condition
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            columns = tuple(columns_list)
            qry = '''SELECT {columns} FROM {table}'''.format(columns=', '.join(map(str, columns)),
                                                             table=table)
            cursor.execute(qry)
            rows = cursor.fetchall()
            # log.debug("Fetching specific columns with out condition was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            # log.exception("Error Fetching Records - {}".format(str(e)))
            raise Exception("Error while fetching records")
        finally:
            cursor.close()

    def delete_records(self, table, delete_condition):
        """
            This function is to delete records from table based on condition
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            qry = '''DELETE FROM {table} where {condition}'''.format(table=table, condition=delete_condition)
            cursor.execute(qry)
            self.db.commit()
            # log.debug("Deleted records based on condition from table successfully")
        except Exception as e:
            self.db.rollback()
            # log.exception("Error while deleting Records - {}".format(str(e)))
            raise Exception("Error while deleting records")
        finally:
            cursor.close()

    def execute_select_query(self, qry):
        """
            This function is to execute select query
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            cursor.execute(qry)
            rows = cursor.fetchall()
            #  log.debug("Executing select query was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            #   log.exception("Error executing select query - {}".format(str(e)))
            raise Exception("Error executing query")
        finally:
            cursor.close()

    def execute_query(self, qry, required):
        """
            This function is to execute a given query
            :param :
            :return:
        """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            cursor.execute(qry)
            rows = None
            self.db.commit()
            if required:
                rows = cursor.fetchall()
                return rows
            else:
                cursor.close()
                print("Executing query was Successful")
        except Exception as e:
            self.db.rollback()
            #    log.exception("Error executing query - {}".format(str(e)))
            raise Exception("Error executing the query")
        finally:
            cursor.close()

    def join(self, tables_list, columns, join_condition, filter_condition):
        """
             This function is to fetch records by joining tables
             :param :
             :return:
         """
        cursor = self.db.cursor()
        try:
            cursor = self.db.cursor()
            columns_list = []
            for each_table in tables_list:
                each_table_columns = columns.get(each_table, [])
                for each_column in each_table_columns:
                    column = each_table + "." + each_column
                    columns_list.append(column)

            columns_tuple = tuple(columns_list)
            tables_tuple = tuple(tables_list)
            if not filter_condition:
                condition = join_condition
            else:
                condition = join_condition + ' and ' + filter_condition
            qry = '''SELECT {columns} FROM {tables} WHERE {condition}'''.format(
                columns=', '.join(map(str, columns_tuple)),
                tables=', '.join(map(str, tables_tuple)), condition=condition)
            cursor.execute(qry)
            rows = cursor.fetchall()
            #     log.debug("Fetching columns by joining tables is Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            #      log.error("Error Joining tables - {}".format(str(e)))
            raise Exception("Error while joining tables")
        finally:
            cursor.close()

    def insert_into_table(self, table, value_list, columns_list):
        cursor = self.db.cursor()
        try:
            print(table)
            print(value_list)
            print(columns_list)
            cursor = self.db.cursor()
            columns = tuple(columns_list)
            values = tuple(value_list)
            qry = '''INSERT INTO {table} ({columns}) VALUES ({values}) '''.format(columns=', '.join(map(str, columns)),
                                                                                  table=table,
                                                                                  values=", ".join(map(str, values)))
            print(qry)
            cursor.execute(qry)
            rows = cursor.fetchall()
            #       log.debug("Inserting record into table was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
      #      traceback.print_exc()
            #        log.exception("Error Inserting record into table - {}".format(str(e)))
            raise Exception("Error while Inserting record into table")
        finally:
            cursor.close()

    def update_status(self, table, uid, status_id, url=None):
        """

        :param table:
        :param uid:
        :param status_id:
        :param url:
        :return:
        """
        cursor = self.db.cursor()
        try:
            qry = '''UPDATE {table} SET {status_id},{modified_on} WHERE {uid}'''.format(
                table=table, status_id=status_id, modified_on=datetime.now(), uid=uid)
            cursor.execute(qry)
            rowcount = cursor.rowcount
            self.db.commit()
            #         log.debug("Status update is successful")
            return rowcount
        except Exception as e:
            self.db.rollback()
            #          log.exception("Error updating table - {}".format(str(e)))
            raise Exception("Error while updating table")
        finally:
            cursor.close()

        # update the table with the current status of the document with id = uid
        # uid, status, update time,

    def fetch_all_record_with_condition(self, table, condition):
        """
            This function is to fetch all records with a condition
            :param :
            :return:
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        try:
            qry = """SELECT * FROM """ + table + """ WHERE """ + condition + """;"""
            cursor.execute(qry)
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            self.db.rollback()
            raise Exception("Error while fetching records")
        finally:
            cursor.close()

    def fetch_all_record(self, table):
        """
            This function is to fetch all records in a table
            :param :
            :return:
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        try:
            qry = """SELECT * FROM """ + table + """;"""
            print(qry)
            cursor.execute(qry)
            rows = cursor.fetchall()
            print(rows)
            #           log.debug("Fetching all records was Successful")
            return rows
        except Exception as e:
            self.db.rollback()
            print(e)
            #            log.exception("Error Fetching Records - {}".format(str(e)))
            raise Exception("Error while Fetching Records")
        finally:
            cursor.close()
