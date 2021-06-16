import glob
import findspark
import datetime
from pyspark.sql import SparkSession
import pyodbc
import os
import time
from pyspark.sql.functions import asc, current_date
from main import connect_to_db
import time


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
    wantedAmount = query.filter(query.categoryID == str(row[1])).select('amount')
    wantedProductID = list(wantedProductID.toPandas()['productID'])
    wantedAmount = list(wantedAmount.toPandas()['amount'])
    cursor_site = conn_site.cursor()
    '''for prodID in wantedProductID:
        if not productProcessing(file_path, query, transactionID, prodID, cursor_site,conn_site, calc_time_left):
            # rollback
            return False'''
    for i in range(len(wantedAmount)):
        prodID = wantedProductID[i]
        amount = wantedAmount[i]
        if not productProcessing(file_path, query, transactionID, prodID, amount, cursor_site, conn_site, calc_time_left):
            # rollback
            return False
    return True


def productProcessing(file_path, query, transactionID, wantedProductID, wantedAmount, cursor_site, conn_site, calc_time_left):
    lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
    if lockCursor.fetchone()[0] == 0:
        productLockType = 'noLockExists'
        # now we are taking a writing lock without checking availability in inventory

        string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Write'}'')"
        string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Write'}')"
        cursor_site.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")

        # TAKING WRITE LOCK ###
        cursor_site.execute(string_query_executable)
        conn_site.commit()

    else:
        lockCursor_not_only_count = cursor_site.execute("select distinct lockType from Locks where locks.productID =" + str(wantedProductID))
        productLockType = lockCursor_not_only_count.fetchone()[0]

    while productLockType == 'Write':
        #IF WE ENTER HERE WE KNOW THAT THE WRITE LOCK ON THE PRODUCT IS SOMEONE ELSE's.#
        #IF IT WAS OUR WRITE LOCKS WE WOULD HAVE : productLockType = 'noLockExists'#
        if calc_time_left() <= 0:
            return False
        lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
        if lockCursor.fetchone()[0] == 0:
            productLockType = 'noLockExists'
            # now we are taking a writing lock without checking availability in inventory

            string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Write'}'')"
            string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Write'}')"
            cursor_site.execute(
                f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")

            # TAKING WRITE LOCK ###
            cursor_site.execute(string_query_executable)
            conn_site.commit()

        else:
            lockCursor_not_only_count = cursor_site.execute("select distinct lockType from Locks where locks.productID =" + str(wantedProductID))
            productLockType = lockCursor_not_only_count.fetchone()[0]


    ## HERE WER ARE SURE THAT WE HAVE READ OR NONE LockTpe ON THE SPECIFIC ProductID ####
    #####################################################
    if productLockType == 'Read':
        string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Read'}'')"
        string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Read'}')"
        cursor_site.execute(
            f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")
        cursor_site.execute(string_query_executable)
        conn_site.commit()

    # CHECKING SITE INVENTORY
    reading_site_query = "select productID,inventory from productsInventory where productID=" + str(wantedProductID)

    cursor_site.execute(reading_site_query)
    productInventoryValue = cursor_site.fetchone()[1]
    if productInventoryValue < int(wantedAmount):
        print(f"Query {file_path} can not be completed")
        return False

    #############################################
    # requesting writing lock for the whole website
    # we already know that out read lock is inside the lock table
    #############################################
    if productLockType == 'Read':
        number_of_readLocks_cursor = cursor_site.execute("select count(*) from Locks where locks.productID =" + str(wantedProductID))
        number_of_readLocks_on_wantedProduct = number_of_readLocks_cursor.fetchone()[0]
        while number_of_readLocks_on_wantedProduct > 1:
            val = calc_time_left()
            if val <= 0:
                return False
            ##know we have 4 read locks (3 + our read lock)##
            ##we are going to delete the 3 locks that are not our lock just to see if we are going out of the while##
            cursor_site.execute('''DELETE FROM Locks WHERE transactionID='AAA_7' ''')
            cursor_site.execute('''DELETE FROM Locks WHERE transactionID='ABC_3' ''')
            cursor_site.execute('''DELETE FROM Locks WHERE transactionID='RRR_8' ''')
            cursor_site.commit()
            ##DON'T FORGET THE DELETE LINE 200-203##
            number_of_readLocks_cursor = cursor_site.execute("select count(*) from Locks where locks.productID =" + str(wantedProductID))
            number_of_readLocks_on_wantedProduct = number_of_readLocks_cursor.fetchone()[0]

    ## WE ARE THE ONLY ONES WITH READ LOCK ON THE PRODUCT ###
    ## ASKING FOR WRITING LOCK##
    ## TAKING READING LOCK
    ## insert into LOG
    string_query_for_log = f"update Locks set lockType = ''{'Write'}'' where productID = {wantedProductID} and transactionID =''{transactionID}'')"
    string_query_executable = f"update Locks set lockType = ''{'Write'}'' where productID = {wantedProductID} and transactionID ='{transactionID}')"
    cursor_site.execute(
        f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")

    # TAKING WRITE LOCK ###
    cursor_site.execute(string_query_executable)
    conn_site.commit()
    ### NEED TO DELETE OUR READ LOCK HERE ###

    ## UPDATING INVENTORY
    val = calc_time_left()
    if val <= 0:
        return False

    string_query = f"UPDATE ProductsInventory SET inventory = Inventory - {wantedAmount} WHERE ProductID={wantedProductID}"
    cursor_site.execute("INSERT INTO Log(timestamp, transactionID, productID, action, record) VALUES (?,?,?,?,?)",
                        (current_date, 'ProductsInventory', transactionID, wantedProductID, 'update', string_query))
    cursor_site.execute(string_query)
    return True


def func_cal_time_left(T, initial_time):
    def calc_time_left():
        return T - (time.time() - initial_time)
    return calc_time_left
