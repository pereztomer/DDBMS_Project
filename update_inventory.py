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


def refill_inventory(cursor, transactionID, T=260):
    for i in range (1, constants.Y+1):
        Y = str(i)
        lock_table = cursor.execute('Select count (distinct LockType) from Locks where productID =' + str(i))
        number_of_locks = cursor.fetchone()[0]
        if number_of_locks == 0:
            log_record_write_lock = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{Y},''{'Write'}'')"
            '''Calling the update_log Function to inform we gona take lock'''
            update_log(cursor=cursor, _relation='Locks', _transactionID=transactionID, _productID=constants.Y,
                       _action='insert', _record=log_record_write_lock)
            '''Actually Taking Write Lock on all product (=every iteration)'''
            # cursor.execute(
            #     f"insert into Locks(transactionID, productID, lockType) VALUES ('{transactionID}',{Y},'{'Write'}')")
            if i == 1:
                continue
            '''WE HAVE THE LOCK + WE INFORM THE LOG = WE REFILL THE INVENTORY'''
            inserting_query = f"UPDATE productsInventory SET Inventory = {constants.P_AMOUNT} Where productID ={i}"
            cursor.execute(inserting_query)
            cursor.execute(
                f"INSERT INTO Log(timestamp ,relation, transactionID,productID,action,record) VALUES ('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'productsInventory'}','{transactionID}',{i},'{'insert'}','{inserting_query}')")
    complementary_inventory = f"update productsInventory set Inventory = {constants.COMPLEMENTARY_AMOUNT} where productID = 1"
    cursor.execute(complementary_inventory)


'''Update the Log Table'''


def update_log(cursor, _relation, _transactionID, _productID, _action, _record):
    log_query = f"insert into Log(timestamp, relation, transactionID, ProductID, action, record) VALUES('{time.strftime('%Y-%m-%d %H:%M:%S')}','{'Locks'}', '{_transactionID}',{_productID},'{_action}','{_record}')"
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
            log_record_write_lock = f"insert into Locks(transactionID,ProductID,lockType) VALUES(''{transactionID}'',{i},''{'insert'}'')"
            cursor.execute("INSERT INTO productsInventory(productID, inventory) VALUES (?,?)",
                           (i, constants.P_AMOUNT))
            conn.commit()
            update_log(cursor, 'Locks', transactionID, i, 'insert', log_record_write_lock)
            '''Actually Taking Write Lock on all product (=every iteration)'''
            conn.commit()
            cursor.execute(
                f"insert into Locks(transactionID,productID,lockType) VALUES ('{transactionID}', {i}, 'Write')")
            conn.commit()
        '''Create the Product with ID == 1 (THE COMPLEMENTARY)'''
        complementary_inventory = f"update productsInventory set Inventory = {constants.COMPLEMENTARY_AMOUNT} where productID = 1"
        cursor.execute(complementary_inventory)
        conn.commit()
    '''Refill our Inventory'''
    refill_inventory(cursor, transactionID)
    conn.commit()
    release_locks = f"DELETE FROM Locks where Locks.transactionID = '{transactionID}'"
    cursor.execute(release_locks)
    conn.commit()


if __name__ == '__main__':
    update_inventory("test")

