import glob
import findspark
import datetime
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
            wantedProductID = list(query.select('productID').toPandas()['productID'])
            wantedProductID_str = '(' + ','.join(str(e) for e in wantedProductID) + ')'
            # relevant categories of query
            categories = list(query.select('categoryID').toPandas()['categoryID'])
            categories = '(' + ','.join(str(e) for e in categories) + ')'
            conn = connect_to_db('dbteam')
            cursor = conn.cursor()
            site_to_rollback = []
            cursor.execute('select distinct siteName, categoryID from CategoriesToSites where categoryID in'+categories)
            site_flag = True
            rollback_flag = False
            for row in cursor:
                site_to_rollback.append(row)
                if not site_flag:
                    rollback_flag = True
                    break
                site_flag = siteProcessing(row, query, file_path, transactionID, calc_time_left)

            if rollback_flag:
                # We do not need to obtain the locks again because we kept them
                # We  do not need lock for log table
                for site, categoryID in site_to_rollback:
                    rollback_conn = connect_to_db(site)
                    rollback_cursor = rollback_conn.cursor()
                    # str_qr = "select * from Log  where Log.transactionID = " + transactionID
                    string_query = "UPDATE ProductsInventory SET inventory = inventory-10 WHERE ProductID=4"
                    ts = datetime.datetime.now()
                    #rollback_cursor.execute(string_query)
                    #rollback_cursor.execute("INSERT INTO Log(timestamp, relation, transactionID, productID, action, record) VALUES (?,?,?,?,?,?)", (ts, 'ProductsInventory', transactionID, wantedProductID[0], 'update', string_query))
                    #rollback_conn.commit()
                    rollback_cursor.execute("select * from Log where Log.transactionID = '"+transactionID + "' AND Log.productID in "+wantedProductID_str +" AND Log.action = 'update' ")
                    # rollback_cursor.execute("DELETE FROM Log WHERE rowID = 786")
                    # rollback_cursor.execute("DELETE FROM Log WHERE rowID = 787")
                    # rollback_cursor.execute("DELETE FROM Log WHERE rowID = 788")
                    # rollback_cursor.execute("DELETE FROM Log WHERE rowID = 789")
                    for rollback_row in rollback_cursor:
                        rollback_query = rollback_row[6]
                        rollback_query = rollback_query.replace('-', '+')
                        rollback_cursor.execute(rollback_query)
                        rollback_conn.commit()






            for row in cursor:
                pass


        #### now we have all th relevant locks for a transaction!
        ##### REALEASE WRITE LOCK ####
        # cursor_site.execute("DELETE FROM Locks where Locks.transactionID == transactionID AND Locks.ProductID==ProductID")


def siteProcessing(row, query, file_path, transactionID, calc_time_left):
    conn_site = connect_to_db(row[0])
    wantedProductID = query.filter(query.categoryID == str(row[1])).select('productID')
    wantedProductID = list(wantedProductID.toPandas()['productID'])
    cursor_site = conn_site.cursor()
    for prodID in wantedProductID:
        if not productProcessing(file_path, query, transactionID, prodID, cursor_site,conn_site, calc_time_left):
            # rollback
            return False
    return True


def productProcessing(file_path, query, transactionID, wantedProductID, cursor_site, conn_site, calc_time_left):
    lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
    if lockCursor.fetchone()[0] == 0:
        productLockType = 'noLockExists'
        # now we are taking a writing lock without checking availability in inventory
        string_query = '''INSERT INTO Locks(transactionID, productID, lockType) VALUES (?,?,?)
                            ''', (transactionID, wantedProductID, 'Write')
        cursor_site.execute('''INSERT INTO Log(timestamp,relation, transactionID, productID, action, record) VALUES (?,?,?,?,?,?)
                            ''', (current_date(), 'Locks', transactionID, wantedProductID, 'Write', 'papa'))

        ## TAKING WRITE LOCK ###
        cursor_site.execute(string_query)
        conn_site.commit()
        exit()

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
    # string_query = ("UPDATE ProductsInventory SET inventory = ? WHERE ProductID=?", Inventory - amount, wantedProductID)
    string_query = "UPDATE ProductsInventory SET inventory = Inventory - amount WHERE ProductID=wantedProductID"
    cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                        (current_date, 'ProductsInventory', transactionID, wantedProductID, 'update', string_query))
    cursor_site.execute(string_query)
    return True


def func_cal_time_left(T, initial_time):
    def calc_time_left():
        return T - (time.time() - initial_time)
    return calc_time_left()
