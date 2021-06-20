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
            rollback_flag = False
            for row in cursor:
                site_to_rollback.append(row)
                site_flag = siteProcessing(row, query, transactionID, calc_time_left)
                if site_flag == False:
                    rollback_flag = True
                    break


            if rollback_flag:
                # We do not need to obtain the locks again because we kept them
                # We  do not need a lock for log table
                for site, categoryID in site_to_rollback:
                    rollback_conn = connect_to_db(site)
                    rollback_cursor = rollback_conn.cursor()
                    rollback_cursor.execute(f"select * from Log where transactionID = '{transactionID}' AND productID in {wantedProductID_str} AND action = '{'update'}' and relation='{'productsInventory'}'")
                    for rollback_row in rollback_cursor:
                        inner_rollback_conn = connect_to_db(site)
                        inner_rollback_cursor = inner_rollback_conn.cursor()
                        rollback_query = rollback_row[6]
                        rollback_query = rollback_query.replace('-', '+')
                        inner_rollback_cursor.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'ProductsInventory'}','{rollback_row[3]}',{rollback_row[4]},'{'update'}','{rollback_query}')")
                        inner_rollback_cursor.execute(rollback_query)
                        inner_rollback_conn.commit()
            else:
                print(f"Transaction {transactionID} completed")
                for site_to_connect in site_to_rollback:
                    wantedProductID = query.filter(query.categoryID == str(site_to_connect[1])).select('productID')
                    wantedAmount = query.filter(query.categoryID == str(site_to_connect[1])).select('amount')
                    wantedProductID = list(wantedProductID.toPandas()['productID'])
                    wantedAmount = list(wantedAmount.toPandas()['amount'])
                    for i in range(len(wantedAmount)):
                        prod = wantedProductID[i]
                        amount = wantedAmount[i]
                        inner_update_ProductsOrdered_conn = connect_to_db(site_to_connect[0])
                        inner_update_ProductsOrdered_cursor = inner_update_ProductsOrdered_conn.cursor()
                        query_update_ProductsInventory_for_log = f"INSERT INTO ProductsOrdered(transactionID,productID,amount) VALUES (''{transactionID}'',{prod},{amount})"
                        query_update_ProductsInventory_executable = f"INSERT INTO ProductsOrdered(transactionID,productID,amount) VALUES ('{transactionID}',{prod},{amount})"
                        log_query_update_productOrdered = f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'ProductsOrdered'}','{transactionID}',{prod},'{'insert'}','{query_update_ProductsInventory_for_log}')"
                        inner_update_ProductsOrdered_cursor.execute(log_query_update_productOrdered)
                        inner_update_ProductsOrdered_cursor.execute(query_update_ProductsInventory_executable)
                        inner_update_ProductsOrdered_conn.commit()



            # we still have all the relevant locks for a transaction!
            # Release write locks for a query

            for site, categoryID in site_to_rollback:
                delete_locks_conn = connect_to_db(site)
                cursor_delete_locks = delete_locks_conn.cursor()
                delete_lock_query_for_log = f"DELETE FROM Locks where Locks.transactionID = ''{transactionID}''"
                delete_lock_query_executable = f"DELETE FROM Locks where Locks.transactionID = '{transactionID}'"
                cursor_delete_locks.execute(f"SELECT DISTINCT(productID) from Log WHERE Log.transactionID = '{transactionID}' AND Log.action = '{'insert'}'")
                for prodID in cursor_delete_locks:
                    inner_delete_locks_conn = connect_to_db(site)
                    inner_cursor_delete_locks = inner_delete_locks_conn.cursor()
                    log_query = f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{prodID[0]},'{'delete'}','{delete_lock_query_for_log}')"
                    inner_cursor_delete_locks.execute(log_query)
                    inner_delete_locks_conn.commit()
                cursor_delete_locks.execute(delete_lock_query_executable)
                delete_locks_conn.commit()


def siteProcessing(row, query, transactionID, calc_time_left):
    wantedProductID = query.filter(query.categoryID == str(row[1])).select('productID')
    wantedAmount = query.filter(query.categoryID == str(row[1])).select('amount')
    wantedProductID = list(wantedProductID.toPandas()['productID'])
    wantedAmount = list(wantedAmount.toPandas()['amount'])
    for i in range(len(wantedAmount)):
        prodID = wantedProductID[i]
        amount = wantedAmount[i]
        if not productProcessing(transactionID, prodID, amount, row[0], calc_time_left):
            # rollback
            return False
    return True


def productProcessing(transactionID, wantedProductID, wantedAmount, site, calc_time_left):
    conn_site = connect_to_db(site)
    cursor_site = conn_site.cursor()
    lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
    if lockCursor.fetchone()[0] == 0:
        catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site)
        productLockType = 'noLockExists'
    else:
        lockCursor_not_only_count = cursor_site.execute("select distinct lockType from Locks where locks.productID =" + str(wantedProductID))
        productLockType = lockCursor_not_only_count.fetchone()[0]
        if productLockType.lower() == 'read':
            catch_read_lock(transactionID, wantedProductID, cursor_site, conn_site)

    #  The products is locked by a write lock
    #  or the product is locked by a read lock and we also have a read lock on it

    while productLockType.lower() == 'write':
        if calc_time_left() <= 0:
            return False
        site_conn = connect_to_db(site)
        cursor_site = site_conn.cursor()
        lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
        sum = 0
        for item in lockCursor:
            sum += item[0]
        if sum == 0:
            # no locks for the product
            catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site)
            productLockType = 'noLockExists'
        else:
            conn_site = connect_to_db(site)
            cursor_site = conn_site.cursor()
            lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
            sum = 0
            for item in lockCursor:
                sum += item[0]
            if sum == 0:
                catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site)
                productLockType = 'noLockExists'
            else:
                lockCursor_not_only_count = cursor_site.execute("select distinct lockType from Locks where locks.productID =" + str(wantedProductID))
                productLockType = lockCursor_not_only_count.fetchone()[0]
                if productLockType.lower() == 'read':
                    catch_read_lock(transactionID, wantedProductID, cursor_site, conn_site)

    # now we have a read lock or write lock

    if not_enough_product(cursor_site, wantedProductID, wantedAmount):
        print(f"Transaction {transactionID} can not be completed")
        return False

    if productLockType.lower() == 'read':
        number_of_readLocks_cursor = cursor_site.execute("select count(*) from Locks where locks.productID =" + str(wantedProductID))
        number_of_readLocks_on_wantedProduct = number_of_readLocks_cursor.fetchone()[0]
        while number_of_readLocks_on_wantedProduct > 1:
            val = calc_time_left()
            if val <= 0:
                return False
            number_of_readLocks_cursor = cursor_site.execute("select count(*) from Locks where locks.productID =" + str(wantedProductID))
            number_of_readLocks_on_wantedProduct = number_of_readLocks_cursor.fetchone()[0]
        update_read_to_write(transactionID, wantedProductID, cursor_site, conn_site)

    # now we have a write lock on the product
    val = calc_time_left()
    if val <= 0:
        return False
    conn_site = connect_to_db(site)
    cursor_site = conn_site.cursor()
    lockCursor = cursor_site.execute("select count(distinct lockType) from Locks where locks.productID =" + str(wantedProductID))
    sum = 0
    for item in lockCursor:
        sum += item[0]
    if sum == 0:
        return False
    if sum != 0:
        lockCursor_not_only_count = cursor_site.execute("select distinct lockType from Locks where locks.productID =" + str(wantedProductID))
        productLockType = lockCursor_not_only_count.fetchone()[0]
        if productLockType.lower() == 'read':
            return False
    else:
        update_inventory(cursor_site, conn_site, wantedAmount, wantedProductID, transactionID)
        return True


def catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site):
    string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Write'}'')"
    string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Write'}')"
    cursor_site.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")

    # TAKING WRITE LOCK ###
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def catch_read_lock(transactionID, wantedProductID, cursor_site, conn_site):
    string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Read'}'')"
    string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Read'}')"
    cursor_site.execute( f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def update_read_to_write(transactionID, wantedProductID, cursor_site, conn_site):
    string_query_for_log = f"update Locks set lockType = ''{'Write'}'' where productID = {wantedProductID} and transactionID =''{transactionID}''"
    string_query_executable = f"update Locks set lockType = '{'Write'}' where productID = {wantedProductID} and transactionID ='{transactionID}'"
    cursor_site.execute( f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'update'}','{string_query_for_log}')")
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def not_enough_product(cursor_site, wantedProductID, wantedAmount):
    reading_site_query = "select productID,inventory from productsInventory where productID=" + str(wantedProductID)
    cursor_site.execute(reading_site_query)
    productInventoryValue = cursor_site.fetchone()[1]
    if productInventoryValue < int(wantedAmount):
        return True
    else:
        return False


def update_inventory(cursor_site, conn_site, wantedAmount, wantedProductID, transactionID):
    #print("update inventory with wantedProductID = " + str(wantedProductID) + "and wanted Amount = " + str(wantedAmount) + "and transactionID = " + str(transactionID))
    string_query = f"UPDATE ProductsInventory SET inventory = Inventory - {wantedAmount} WHERE ProductID={wantedProductID}"
    #print("1")
    cursor_site.execute(
        f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'ProductsInventory'}','{transactionID}',{wantedProductID},'{'update'}','{string_query}')")
    #print("2")
    cursor_site.execute(string_query)
    #print("3")
    conn_site.commit()
    #print("4")

def func_cal_time_left(T, initial_time):
    def calc_time_left():
        return T - (time.time() - initial_time)
    return calc_time_left


