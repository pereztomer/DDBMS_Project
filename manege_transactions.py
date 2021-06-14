import glob
import findspark
from pyspark.sql import SparkSession
import pyodbc
import os
import time
from pyspark.sql.functions import asc, current_date
from main import connect_to_db


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def lock_directory(directory_pass='C:/Users/Tomer/PycharmProjects/DDBMS_Project/orders/'):
    pass


def manege_transactions(T):
    calc_time_left = func_cal_time_left(T, time.time())
    dir_name = 'orders/'
    list_of_files = sorted(filter(os.path.isfile, glob.glob(dir_name + '*')))
    lock_directory()
    if len(list_of_files) == 0:
        print("Currently no queries available")
        return

    spark, sc = init_spark("manege_transactions")

    for file_path in list_of_files:
        # NEEDS TO CHECK IF THE FILE IS ACCORDING TO FORMAT!!
        if not file_path.endswith('_11.csv'):
            print(f"File {file_path} not in correct format - was rejected")
        else:
            transactionID = os.path.splitext(os.path.basename(file_path))[0]
            query = spark.read.format("csv").option("header", "true").load(file_path)
            # relevant categories of query
            categories = list(query.select('categoryID').toPandas()['categoryID'])
            categories = '(' + ','.join(str(e) for e in categories) + ')'
            conn = connect_to_db('dbteam')
            cursor = conn.cursor()
            cursor.execute(
                'select distinct siteName, categoryID from CategoriesToSites where categoryID in' + categories)
            site_flag = True
            for row in cursor:
                if not site_flag:
                    # rollback!
                    pass
                site_flag = siteProcessing(row, query, file_path, transactionID, calc_time_left)

            # query processing or inside site processing
            # rollback
        # checking if all sites were successfully if not rollback


def siteProcessing(row, query, file_path, transactionID, calc_time_left):
    conn_site = connect_to_db(row[0])
    wantedProductID = query.filter(query.categoryID == str(row[1])).select('productID')
    wantedProductID = list(wantedProductID.toPandas()['productID'])
    wantedProductID = '(' + ','.join(str(e) for e in wantedProductID) + ')'
    cursor_site = conn_site.cursor()
    for prodID in wantedProductID:
        if not productProcessing(file_path, query, transactionID, prodID, cursor_site, calc_time_left):
            # rollback
            return False
    return True


def productProcessing(file_path, query, transactionID, wantedProductID, cursor_site, calc_time_left):
    lockCursor = cursor_site.execute('select distinct lockType from Locks where locks.productID = wantedProductID')
    if lockCursor.count() == 0:
        productLockType = 'noLockExists'
        # now we are taking a writing lock without checking availability in inventory
        string_query = str(
            ("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID, wantedProductID,
             'Write'))
        cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                            (current_date(), 'Locks', transactionID, wantedProductID, 'Write', string_query))
        ## TAKING WRITE LOCK ###
        cursor_site.execute("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID,
                            wantedProductID, 'Write')
    else:
        # we don't know which type of lock it is
        lockTypeTable = list(lockCursor.select('lockType').toPandas()['lockType'])
        productLockType = '(' + ','.join(str(e) for e in lockTypeTable) + ')'

    while productLockType == 'write':
        if calc_time_left() <= 0:
            return False
        lockCursor = cursor_site.execute('select distinct lockType from Locks where lock.productID = wantedProductID')
        if lockCursor.count() == 0:
            productLockType = 'noLockExists'
            string_query = str(
                ("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID, wantedProductID,
                 'Write'))
            cursor_site.execute(
                "INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                (current_date(), 'Locks', transactionID, wantedProductID, 'Write', string_query))
            ## TAKING WRITE LOCK ###
            cursor_site.execute("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID,
                                wantedProductID, 'Write')
        else:
            productLockType = list(lockCursor.select('lockType').toPandas()['lockType'])[0]

    ## HERE WER ARE SURE THAT WE HAVE READ OR NONE LockTpe ON THE SPECIFIC ProductID ####
    #####################################################
    if productLockType == 'read':
        string_query = str(
            ("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID, wantedProductID,
             'Read'))
        # insert into LOG
        cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                            (current_date, 'ProductsOrdered', transactionID, wantedProductID, 'Read', string_query))
        # TAKING READING LOCK
        cursor_site.execute("INSERT INTO Locks(transactionID, ProductID,lockType) VALUES (?,?,?)", transactionID,
                            wantedProductID, 'Read')

    # CHECKING SITE INVENTORY
    reading_site_query = 'select productID,inventory from productsInventory where productID in' + wantedProductID

    cursor_site.execute(reading_site_query)

    for user_row in cursor_site:
        product_amount = query.filter(query.productID == int(user_row[1])).select('amount')
        product_amount_lst = list(product_amount.toPandas()['amount'])
        product_amount = sum(product_amount_lst)
        if user_row[1] < product_amount:
            print(f"Query {file_path} can not be completed")
            return False

    #############################################
    # requesting writing lock for the whole website
    # we already know that out read lock is inside the lock table
    #############################################
    if productLockType == 'read':
        number_of_readLocks = cursor_site.execute(
            'select * from Locks where lock.productID = wantedProductID')
        while number_of_readLocks.count() > 1:
            if calc_time_left() <= 0:
                return False
            number_of_readLocks = cursor_site.execute(
                'select * from Locks where lock.productID = wantedProductID')
    ## WE ARE THE ONLY ONES WITH READ LOCK ON THE PRODUCT ###
    ## ASKING FOR WRITING LOCK##
    ## TAKING READING LOCK
    ## insert into LOG
    string_query = "UPDATE Locks SET LockType= 'Write' WHERE ProductID=wantedProductID"
    cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                       (current_date, 'Locks', transactionID, wantedProductID, 'update', string_query))
    ## Updating to write lock ###
    cursor_site.execute(string_query)

    ## UPDATING INVENTORY
    if calc_time_left() <= 0:
        return False
    string_query = "UPDATE ProductsInventory SET inventory = Inventory - amount WHERE ProductID=wantedProductID"
    cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                        (current_date, 'ProductsInventory', transactionID, wantedProductID, 'update', string_query))
    cursor_site.execute(string_query)

    ##### REALEASE WRITE LOCK ####
    cursor_site.execute("DELETE FROM Locks where Locks.transactionID == transactionID AND Locks.ProductID==ProductID")
    return True


def func_cal_time_left(T, initial_time):
    def calc_time_left():
        return T - (time.time() - initial_time)
    return calc_time_left()
