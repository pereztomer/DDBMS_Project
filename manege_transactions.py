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
    username = username
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
        transacionID = file_path.replace('C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/', '').replace('_11.csv','')
        if not file_path.endswith('_' + str(X) + '.csv'):
            print(f"File {file_path} not in correct format - was rejected")
        else:
            query = spark.read.format("csv").option("header", "true").load(file_path)
            categories = list(query.select('categoryID').toPandas()['categoryID'])
            categories = '(' + ','.join(str(e) for e in categories) + ')'
            conn = connect_to_db('dbteam')
            cursor = conn.cursor()
            cursor.execute('select distinct siteName, categoryID from CategoriesToSites where categoryID in' + categories)
            flag = True
            for row in cursor:
                if not flag:
                    break

                conn_row = connect_to_db(row[0])
                wantedProductID = query.filter(query.categoryID == str(row[1])).select('productID')
                wantedProductID = list(wantedProductID.toPandas()['productID'])
                wantedProductID = '(' + ','.join(str(e) for e in wantedProductID) + ')'
                cursor_row = conn_row.cursor()
                #########################################
                # We need a reading lock here!!!!
                ##########################################
                reading_site_query = 'select productID,inventory from productsInventory where productID in' + wantedProductID
                cursor_row.execute('INSERT INTO Log (timestamp, relation, transactionID, productID, action, record) \
                 VALUES (current_date ,ProductsOrdered, transactionID, productID, read,reading_site_query)')
                cursor_row.execute(reading_site_query)
                # האם יש מקרה קצה בו עבור אותה קטגוריה ומוצר יש כמה דרישות בcsv?
                for user_row in cursor_row:
                    product_amount = query.filter(query.productID == int(user_row[1])).select('amount')
                    product_amount_lst = list(product_amount.toPandas()['amount'])
                    product_amount = sum(product_amount_lst)
                    if user_row[1] < product_amount:
                        print(f"Query {file_path} can not be completed")
                        ###############################################
                        # a rollback mechanism needs to be created here
                        ###############################################
                        flag = False
                        break

                #############################################
                # requesting writing lock for the whole website
                #############################################
                # update log of accessing to productordered
                print("hello")

                cursor_row.execute('INSERT INTO Log (timestamp, relation, transactionID, productID, action, record) '
                                   'VALUES (current_date ,ProductsOrdered, transactionID, productID, 'read',reading_site_query   )')
                # update procutsordered of user with(our_tID,pID,ourAmount)
                # update log of down inventory
                # update productsinventory with inventory = inventory - ourAmount


        #######################################################
        # checking if all sites were successfully if not rollback
        #######################################################