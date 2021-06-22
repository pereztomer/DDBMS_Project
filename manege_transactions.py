import glob
from pyspark.sql import SparkSession
import pyodbc
import os
import time


# Constants
# products id
X = 11
# num of products
Y = 6

Z = 59

# we have 590 PRODUCTS IN TOTAL

# num of products for each product except 1/-1
P_AMOUNT = (Z * 10) // Y

COMPLEMENTARY_AMOUNT = Z*10 - P_AMOUNT * (Y-1) ## amount of category 1


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


def create_tables():
    conn = connect_to_db("rubensasson")
    cursor = conn.cursor()
    cursor.execute('''
                      Create table ProductsInventory(
                          productID integer primary key,
                          inventory integer,
                          CHECK (inventory >= 0)
                      );''')
    cursor.execute('''
                      Create table ProductsOrdered(
                          transactionID varchar(30),
                          productID integer ,
                          amount integer ,
                          primary key(transactionID,productID),
                          foreign key (productID) references ProductsInventory(productID)
                      );''')
    cursor.execute('''
                      Create table Log(
                          rowID int identity(1,1) primary key,
                          timestamp datetime,
                          relation varchar(20),
                          transactionID varchar(30),
                          productID integer,
                          action varchar(10),
                          record varchar(2500),
                          check(action='insert' or action='delete' or action='update' or action='read'),
                          foreign key (productID) references ProductsInventory(productID),
                          check(relation='ProductsInventory' or relation='Locks' or relation='ProductsOrdered')
                      );''')
    cursor.execute('''
                      Create table Locks(
                          transactionID varchar(30),
                          productID integer,
                          lockType varchar(10),
                          foreign key(productID) references productsInventory(productID),
                          primary key(transactionID,productID,lockType)
                      );''')

    conn.commit()


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


'''Update the Log Table'''


def update_log(cursor, _relation, _transactionID, _productID, _action, _record):
    log_query = f"insert into Log(timestamp, relation, transactionID, ProductID, action, record) VALUES('{time.strftime('%Y-%m-%d %H:%M:%S')}','{_relation}', '{_transactionID}',{_productID},'{_action}','{_record}')"
    cursor.execute(log_query)


def update_inventory(transactionID):
    # needs to check if we need to update log table
    conn = connect_to_db('rubensasson')
    cursor = conn.cursor()
    cursor.execute('select count(*) from productsInventory')
    number_of_product = cursor.fetchone()[0]
    '''Our Table is Empty (First Initiation)'''
    if number_of_product == 0:
        '''It means that the DB wasn't Init so there can't be Locks because there are no PRODUCTS!'''
        for i in range(1, Y + 1):
            ''' updating the inventory for the specific product'''
            update_inventory_query = f"INSERT INTO productsInventory(productID, inventory) VALUES ({i}, {P_AMOUNT})"
            update_inventory_query_log = f"INSERT INTO productsInventory(productID, inventory) VALUES ({i}, {P_AMOUNT})"
            cursor.execute(update_inventory_query)
            conn.commit()

            '''taking the write lof for a a specific pID ### MUST BE AFTER THE INSERTO productsInventory bcs of KEYS ###'''
            write_lock_query = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{i},'{'insert'}')"
            write_lock_query_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{i},''{'insert'}'')"
            cursor.execute(write_lock_query)
            conn.commit()

            '''Updating the log table with PorductsInventory Update + Locks Table Update'''
            if i == 1:
                continue
            update_log(cursor, 'Locks', transactionID, i, 'insert', write_lock_query_log)
            update_log(cursor, 'productsinventory', transactionID, i, 'insert', update_inventory_query_log)
            conn.commit()

        '''Create the Product with ID == 1 (THE COMPLEMENTARY)'''
        complementary_inventory = f"update productsInventory set Inventory = {COMPLEMENTARY_AMOUNT} where productID = 1"
        cursor.execute(complementary_inventory)
        write_lock_query_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'', 1 ,''{'insert'}'')"
        update_log(cursor, 'Locks', transactionID, i, 'insert', write_lock_query_log)
        update_log(cursor, 'productsinventory', transactionID, 1, 'insert', complementary_inventory)
        conn.commit()
    else:
        refill_inventory(cursor, transactionID)
        conn.commit()
    '''Refill our Inventory'''
    conn.commit()
    '''Release all the locks '''
    release_locks = f"DELETE FROM Locks where Locks.transactionID = '{transactionID}'"
    release_locks_log = f"DELETE FROM Locks where Locks.transactionID = ''{transactionID}''"
    cursor.execute(release_locks)
    '''updating the log that we Released all the locks '''
    for i in range(1, Y + 1):
        update_log(cursor, 'Locks', transactionID, i, 'delete', release_locks_log)
    conn.commit()


def refill_inventory(cursor, transactionID, T=100):
    initial_time = time.time()
    for i in range(1, Y + 1):
        #Y = str(i)
        lock_table = cursor.execute('Select count (distinct LockType) from Locks where productID =' + str(i))
        number_of_locks = lock_table.fetchone()[0]

        while number_of_locks != 0 and initial_time - time.time() > (T / Y):
            lock_table = cursor.execute('Select count (distinct LockType) from Locks where productID =' + str(i))
            number_of_locks = lock_table.fetchone()[0]

        '''it means there is no lock on the productID we need'''
        if number_of_locks == 0:
            write_lock_taking_query_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{i},''{'Write'}'')"
            write_lock_taking_query_exec = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{i},'{'Write'}')"
            '''Calling the update_log Function to inform we gona take lock'''
            update_log(cursor=cursor, _relation='Locks', _transactionID=transactionID, _productID=i,
                       _action='insert', _record=write_lock_taking_query_log)
            '''Actually Taking Write Lock on all product (=every iteration)'''
            cursor.execute(write_lock_taking_query_exec)

            if i == 1:
                continue
            '''WE HAVE THE LOCK + WE INFORM THE LOG OF TAKING THE LOCK= WE REFILL THE INVENTORY'''
            inserting_query = f"UPDATE productsInventory SET Inventory = {P_AMOUNT} Where productID ={i}"
            cursor.execute(inserting_query)
            '''updating the log table for reffilling the productID inventory'''
            cursor.execute(
                f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'productsInventory'}','{transactionID}',{i},'{'update'}','{inserting_query}')")
        else:
            '''TIMES UP'''
            print(f"Your request to refill product {i} could not be completed")
    '''Updating the complementary product'''
    complementary_inventory = f"update productsInventory set Inventory = {COMPLEMENTARY_AMOUNT} where productID = 1"
    cursor.execute(complementary_inventory)
    update_log(cursor, 'productsinventory', transactionID, 1, 'update', complementary_inventory)

def manege_transactions(T):
    dir_name = 'orders/'
    list_of_files = sorted(filter(os.path.isfile, glob.glob(dir_name + '*')))
    if len(list_of_files) == 0:
        print("Currently no queries available")
        return

    spark, sc = init_spark("manege_transactions")

    for file_path in list_of_files:
        # NEEDS TO CHECK IF THE FILE IS ACCORDING TO FORMAT!!
        if not file_path.endswith('_11.csv'):
            print(f"File {file_path} not in correct format - was rejected")
        else:
            calc_time_left = func_cal_time_left(T / len(list_of_files), time.time())
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
                print(f"Transaction {transactionID} can not be completed")
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
                        log_query_update_productOrdered = f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'ProductsOrdered'}','{transactionID}',{prod},'{'update'}','{query_update_ProductsInventory_for_log}')"
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
    for i in range(len(wantedProductID)):
        prodID = wantedProductID[i]
        amount = wantedAmount[i]
        if not productProcessing(transactionID, prodID, amount, row[0], calc_time_left):
            # rollback
            return False
    return True


def productProcessing(transactionID, wantedProductID, wantedAmount, site, calc_time_left):
    while calc_time_left() > 10:
        conn_site = connect_to_db(site)
        cursor_site = conn_site.cursor()
        lockCursor = cursor_site.execute(
            f"select distinct lockType from Locks where locks.productID = {wantedProductID} and transactionID != '{transactionID}'")
        temp = lockCursor.fetchone()
        if temp is None:
            get_write_lock(transactionID, wantedProductID, site)
            if enough_product(wantedProductID, wantedAmount, site):
                break
            return False
        elif temp[0].lower() == 'read':
            # maybe there are two results from the query - both read and write
            catch_read_lock(transactionID, wantedProductID, site)
            if enough_product(wantedProductID, wantedAmount,site):
                continue
            return False
        else:
            print(str(transactionID) + " waiting for lock on " + str(wantedProductID))
            # We don't have any lock
            pass

    if calc_time_left() <= 10:
        return False
    else:
        conn_site = connect_to_db(site)
        cursor_site = conn_site.cursor()
        string_query = f"select * from Locks where locks.productID = {wantedProductID} AND locks.transactionID = '{transactionID}' AND locks.lockType = '{'Write'}'"
        write_checking_cursor = cursor_site.execute(string_query)
        r = write_checking_cursor.fetchone()
        noWriteLock = r is None
        if noWriteLock:
            return False
        take_product(cursor_site, conn_site, wantedAmount, wantedProductID, transactionID)
        return True


def get_write_lock(transactionID, wantedProductID, site):
    conn_site = connect_to_db(site)
    cursor_site = conn_site.cursor()
    readLockAlreadyThere = cursor_site.execute(f"select * from Locks where Locks.productId = {wantedProductID} and Locks.lockType = '{'Read'}'")
    if readLockAlreadyThere.fetchone() is None:
        catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site)
    else:
        update_read_to_write(transactionID, wantedProductID, cursor_site, conn_site)


def catch_write_lock(transactionID, wantedProductID, cursor_site, conn_site):
    string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Write'}'')"
    string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Write'}')"
    cursor_site.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")

    # TAKING WRITE LOCK ###
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def update_read_to_write(transactionID, wantedProductID, cursor_site, conn_site):
    string_query_for_log = f"update Locks set lockType = ''{'Write'}'' where productID = {wantedProductID} and transactionID =''{transactionID}''"
    string_query_executable = f"update Locks set lockType = '{'Write'}' where productID = {wantedProductID} and transactionID ='{transactionID}'"
    cursor_site.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'update'}','{string_query_for_log}')")
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def catch_read_lock(transactionID, wantedProductID, site):
    conn_site = connect_to_db(site)
    cursor_site = conn_site.cursor()
    string_query_for_log = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{wantedProductID},''{'Read'}'')"
    string_query_executable = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{wantedProductID},'{'Read'}')"
    cursor_site.execute(f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}','{transactionID}',{wantedProductID},'{'insert'}','{string_query_for_log}')")
    cursor_site.execute(string_query_executable)
    conn_site.commit()


def enough_product(wantedProductID, wantedAmount, site):
    conn_site = connect_to_db(site)
    cursor_site = conn_site.cursor()
    reading_site_query = "select productID,inventory from productsInventory where productID=" + str(wantedProductID)
    cursor_site.execute(reading_site_query)
    productInventoryValue = cursor_site.fetchone()[1]
    if productInventoryValue >= int(wantedAmount):
        return True
    return False


def take_product(cursor_site, conn_site, wantedAmount, wantedProductID, transactionID):
    string_query = f"UPDATE ProductsInventory SET inventory = Inventory - {wantedAmount} WHERE ProductID={wantedProductID}"
    cursor_site.execute(
        f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'ProductsInventory'}','{transactionID}',{wantedProductID},'{'update'}','{string_query}')")
    cursor_site.execute(string_query)
    conn_site.commit()


def func_cal_time_left(T, initial_time):
    def calc_time_left():
        return T - (time.time() - initial_time)
    return calc_time_left


