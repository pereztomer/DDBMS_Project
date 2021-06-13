import glob
import os
import findspark
from pyspark.sql import SparkSession
import pyodbc
import os
import multiprocessing
import time


"""    each process initilize with multiprocessing.process(target = funcitionName)
    to start the function proceess we will use p1.start()
    we can contiou the script after .start()
    join - will wait to be done before the script continou
    for loop to initialize processes
"""
#####

# Constants
X = 11
Y = 6
Z = 59
from pyspark.sql.functions import asc, current_date
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
        transactionID = file_path.baendswith('')
        transactionID = os.path.splitext(os.path.basename(transactionID))[1]

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

                #FIRST PROCESS DISTRIBUTIONS WILL START HERE
                site_p = multiprocessing.process(target= siteProcessing, args = [row, query, file_path, transactionID])
                site_p.start()

        # checking if all sites were successfully if not rollback

def productProcessing(file_path, query, transactionID, productID, cursor_row):
    lockCursor = cursor_row.execute('select distinct lockType from Locks where lock.productID = wantedProductID')
    if lockCursor.count() == 0:
        productLockType = 'noLockExists'
    else:
        lockTypeTable = list(lockCursor.select('lockType').toPandas()['lockType'])
        productLockType = '(' + ','.join(str(e) for e in lockTypeTable) + ')'

    while productLockType == 'write':
        lockCursor = cursor_row.execute(
            'select distinct lockType from Locks where lock.productID = wantedProductID')
        if lockCursor.count() == 0:
            productLockType = 'noLockExists'
        else:
            lockTypeTable = list(lockCursor.select('lockType').toPandas()['lockType'])
            productLockType = '(' + ','.join(str(e) for e in lockTypeTable) + ')'
    ## HERE WER ARE SURE THAT WE HAVE READ OR NONE LockTpe ON THE SPECIFIC ProductID ####
    #####################################################
    string_query = str(
        ("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID, productID, 'Read'))
    ## insert into LOG
    cursor_row.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                       (current_date, 'ProductsOrdered', transactionID, productID, 'Read', string_query))
    ## TAKING READING LOCK
    cursor_row.execute("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID,
                       wantedProductID, 'Read')

    ## CHECKING SITE INVENTORY
    reading_site_query = 'select productID,inventory from productsInventory where productID in' + productID

    cursor_row.execute(reading_site_query)

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
    number_of_readLocks = cursor_row.execute(
        'select * from Locks where lock.productID = wantedProductID')
    while number_of_readLocks.count() > 1:
        number_of_readLocks = cursor_row.execute(
            'select * from Locks where lock.productID = wantedProductID')
    ## WE ARE THE ONLY ONES WITH READ LOCK ON THE PRODUCT ###
    ## ASKING FOR WRITING LOCK##
    ## TAKING READING LOCK
    ## insert into LOG
    cursor_row.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                       (current_date, 'ProductsOrdered', transactionID, productID, 'Write', string_query))
    ## TAKING WRITE LOCK ###
    cursor_row.execute("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID,
                    productID, 'Write')
    ### DELETE READ LOCK FROM Locks
    cursor_row.execute("DELETE FROM Locks where Locks.transactionID == transactionID AND Locks.ProductID==ProductID")

    ## UPDATING INVENTORY
    tmp = cursor_row.execute('select inventory FROM ProductsInventory where ProductsInventory.ProductID==ProductID')
    product_inventory = list(tmp.toPandas()['inventory'])
    product_inventory = product_inventory[0]
    cursor_row.execute("INSERT INTO ProductsInventory(productID, inventory) VALUES (?,?)",
                       (productID, product_inventory - product_amount))
    ##### REALEASE WRITE LOCK ####
    cursor_row.execute("DELETE FROM Locks where Locks.transactionID == transactionID AND Locks.ProductID==ProductID")


def siteProcessing(row, query, file_path, transactionID):
    conn_row = connect_to_db(row[0])
    wantedProductID = query.filter(query.categoryID == str(row[1])).select('productID')
    wantedProductID = list(wantedProductID.toPandas()['productID'])
    wantedProductID = '(' + ','.join(str(e) for e in wantedProductID) + ')'
    cursor_row = conn_row.cursor()
    for prodID in wantedProductID:
        product_p = multiprocessing.Process(target=productProcessing, args=[file_path, query, transactionID, prodID, cursor_row])
        product_p.start()
