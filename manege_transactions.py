import glob
import os
import findspark
from pyspark.sql import SparkSession
import pyodbc


# Constants
X = 11
Y = 6
Z = 59
from pyspark.sql.functions import asc
from pyspark.sql.functions import avg
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

def connect_to_db(username):
    server = 'technionddscourse.database.windows.net'
    database = username
    username = 'rubensasson'
    password = 'Qwerty12!'
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=' + server + ';'
        'DATABASE=' + database + ';'
        'UID=' + username + ';'
        'PWD=' + password + ';')
    return conn







def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def lock_directory(directory_pass='C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/'):
    pass


def manege_transactions(T):
    dir_name = 'C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/'
    list_of_files = sorted(filter(os.path.isfile, glob.glob(dir_name + '*')))
    lock_directory()
    if len(list_of_files) == 0:
        print("Currently no queries available")
        return

    spark, sc = init_spark("manege_transactions")

    for file_path in list_of_files:
        if not file_path.endswith('_' + str(X)+'.csv'):
            print(f"File {file_path} not in correct format - was rejected")
        else:
            query = spark.read.format("csv").option("header", "true").load(file_path)
            query = query.select('categoryID').rdd.map(r >= r(0)).collect()
            print(query)
            exit()
            conn = connect_to_db('dbteam')
            cursor = conn.cursor()
            cursor.execute('select siteName '
                           'from table CategoriesToSites'
                           'where categoryID in (12,12,32)')

            print(query.show())
            exit()
