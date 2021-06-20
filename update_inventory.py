import constants
import manege_transactions
import pyodbc
import time


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


def refill_inventory(cursor, transactionID, T=10):
    initial_time = time.time()
    for i in range(1, constants.Y + 1):
        Y = str(i)
        lock_table = cursor.execute('Select count (distinct LockType) from Locks where productID =' + str(i))
        number_of_locks = lock_table.fetchone()[0]

        while number_of_locks != 0 and initial_time - time.time() > T / constants.Y:
            lock_table = cursor.execute('Select count (distinct LockType) from Locks where productID =' + str(i))
            number_of_locks = lock_table.fetchone()[0]

        '''it means there is no lock on the productID we need'''
        if number_of_locks == 0:
            write_lock_taking_query = f"insert into Locks(transactionID,ProductID,lockType) VALUES('{transactionID}',{i},'{'Write'}')"
            '''Calling the update_log Function to inform we gona take lock'''
            update_log(cursor=cursor, _relation='Locks', _transactionID=transactionID, _productID=i,
                       _action='insert', _record=write_lock_taking_query)
            '''Actually Taking Write Lock on all product (=every iteration)'''
            cursor.execute(write_lock_taking_query)

            if i == 1:
                continue
            '''WE HAVE THE LOCK + WE INFORM THE LOG OF TAKING THE LOCK= WE REFILL THE INVENTORY'''
            inserting_query = f"UPDATE productsInventory SET Inventory = {constants.P_AMOUNT} Where productID ={i}"
            cursor.execute(inserting_query)
            '''updating the log table for reffilling the productID inventory'''
            cursor.execute(
                f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'productsInventory'}','{transactionID}',{i},'{'insert'}','{inserting_query}')")
        else:
            '''TIMES UP'''
            print(f"Your request to refill product {i} could not be completed")
    '''Updating the complementary product'''
    complementary_inventory = f"update productsInventory set Inventory = {constants.COMPLEMENTARY_AMOUNT} where productID = 1"
    cursor.execute(complementary_inventory)
    update_log(cursor, 'productsinventory', transactionID, 1, 'update', complementary_inventory)


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
        for i in range(1, constants.Y + 1):
            ''' updating the inventory for the specific product'''
            update_inventory_query = f"INSERT INTO productsInventory(productID, inventory) VALUES ({i}, {constants.P_AMOUNT})"
            update_inventory_query_log = f"INSERT INTO productsInventory(productID, inventory) VALUES ({i}, {constants.P_AMOUNT})"
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
        complementary_inventory = f"update productsInventory set Inventory = {constants.COMPLEMENTARY_AMOUNT} where productID = 1"
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
    for i in range(1, constants.Y + 1):
        update_log(cursor, 'Locks', transactionID, i, 'insert', release_locks_log)
    conn.commit()


if __name__ == '__main__':
    update_inventory("test")
