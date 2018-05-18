'''
DB_Connection initializes a connection to a database and provides simple methods
for querying data, loading data into a pandas DataFrame, automated
assistance for creating tables from csv or a pandas DataFrame, and interacting
in miscellaneous ways with the database.

@author Vidal Anguiano Jr.
'''
import pandas as pd
import csv, ast, psycopg2
import os
import json
from vatools.src.util import *
from pandas_profiling import ProfileReport


class DB_Connection(object):
    '''
    DB_Connection is a way to initialize a connection to a database and easily
    interact with it with simple functions to query data, create a table, and
    run other SQL commands on the database.
    '''
    def __init__(self, credentials_file = 'credentials.json'):
        '''
        Initializes the DB_Connection by collecting access credentials from a
        'credentials.json' file.
        '''
        creds = json.load(open(credentials_file))
        self.hostname = creds["hostname"]
        self.username = creds["username"]
        self.password = creds["password"]
        self.database = creds["database"]
        self.conn = None


    def connect(self):
        '''
        Initialize a connection to the database.
        '''
        self.conn = psycopg2.connect( host=self.hostname, user=self.username,
                          password=self.password, dbname=self.database )


    def close(self):
        '''
        Close database connection.
        '''
        self.conn.close()


    def query(self, query, pandas = True):
        '''
        Establishes connection to database with provided credentials, takes a
        query, and by default returns the result as a pandas DataFrame.
        Inputs:
            - query (str): a query to interact with the database
            - pandas (bool): default, True. If true, the query result is
            returned as a pandas dataframe. Othersise, it is returned as a
            fetchall
        '''
        conn = self.conn

        assert conn, "Initialize a connection first!"
        if pandas:
            result = read_sql_query(query, conn)
            return result

        cur = conn.cursor()
        cur.execute(query)
        if 'create ' in query.lower() or 'drop ' in query.lower():
            conn.commit()
            return None
        print(cur.fetchall())


    def create_table(self, csv_file, table_name, insert = True, sep = ','):
        '''
        Takes a csv file and automatically detects field types and produces an
        initial Schema DDL, and finally loads the data from the csv file into
        the database. The option is given to modify the DDL before the
        data is loaded into the database.
        Inputs:
            - csv_file (str): path for the csv file to be loaded
        '''
        conn = self.conn
        assert conn, "Initialize a connection first!"
        cur = conn.cursor()
        try:
            cur.execute('drop table if exists {};'.format(table_name))
            conn.commit()
            statement = create_ddl(csv_file, table_name)
            cur.execute(statement)
            print("{} created successfully!".format(table_name))

            if insert:
                with open(csv_file,'r') as f:
                    next(f)
                    cur.copy_from(f, table_name, sep, null = '')
                    conn.commit()
                    print("Data successfully loaded into {}".format(table_name))
                # print(self.query("select * from " + table_name + " limit 2;"))
        except Exception as e:
            print(e)
            conn.close()


    def create_table_from_df(self, df, table_name, sep = ','):
        '''
        Function to load data from a pandas DataFrame into the data base.
        Inputs:
            - df (pandas DataFrame): data to input into database
            - table_name (str): name to give the table where data will be loaded
        '''
        df.to_csv('./temp.csv', index=False, sep=sep)
        self.create_table('temp.csv', table_name, insert=True)
        os.remove('./temp.csv')

    def profile(self, table_name):
        return ProfileReport(self.query('SELECT * FROM {}'.format(table_name)),
                             check_correlation = False)


def dataType(val, current_type):
    '''
    Helper function to detect datatypes.
    '''
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'VARCHAR'
    except SyntaxError:
        return 'VARCHAR'

    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
           # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'SMALLINT'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'INT'
            else:
                return 'BIGINT'
        if type(t) is float and current_type not in ['varchar']:
            return 'FLOAT'
    else:
        return 'VARCHAR'


def create_ddl(file_path, table_name):
    '''
    Helper function to create a DDL statement.
    '''
    f = open(file_path, 'r')
    reader = csv.reader(f)
    longest, headers, type_list = [], [] ,[]
    for row in reader:
        if len(headers) == 0:
            headers = row
            for col in row:
                longest.append(0)
                type_list.append('')
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
    f.close()

    statement = 'CREATE TABLE ' + table_name + ' ('

    for i in range(len(headers)):
        if type_list[i] == 'VARCHAR':
            statement = (statement + '\n{} VARCHAR({}),').format(headers[i].
    lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i]
    .lower(), type_list[i])
        if '-' in statement:
            statement = statement.replace('-','_')

    statement = statement[:-1] + ');'

    print(statement)
    edit = ''
    while edit not in ['Y', 'N']:
        edit = input('Do you want to make any changes? Y/N ').upper()
        edit = edit.upper()

    statement = make_edits(statement, edit)

    return statement


def make_edits(statement, edit):
    '''
    Helper function to process edits to the DDL statement.
    '''
    statement = statement.split('\n')
    last = len(statement)
    if edit == 'Y':
        for i, line in enumerate(statement[1:]):
            attribute, type_ = line.split(' ')[0], line.split(' ')[1]
            s = attribute + " of type " + type_[:-1] + '? '
            fix = ''
            if fix not in ['s','varchar','int','real','smallint','text','char']:
                fix = input(s)
                fix_check = fix.split('(')[0].lower()
            if fix_check in ['y','s','']:
                continue
            else:
                type_ = ' ' + fix.upper() + ','
                statement[i+1] = str(line.split(' ')[0]) + type_
        if ');' not in statement[last-1]:
            statement[last-1] = statement[last-1][:-1] + ');'
        return '\n'.join(statement)
    else:
        return '\n'.join(statement)
