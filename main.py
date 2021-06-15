import pyodbc
import constants
import manege_transactions
import datetime
from pyspark.sql.functions import asc, current_date

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


def functionDrop():
    conn = connect_to_db("rubensasson")
    cursor = conn.cursor()
    #cursor.execute('''
    #                     DROP TABLE Locks;
    #                   ''')
    #conn.commit()
    #cursor.execute('''
    #             DROP TABLE Log;
    #            ''')
    #conn.commit()
    cursor.execute('''
                 DROP TABLE ProductsOrdered;
                ''')
    conn.commit()
    cursor.execute('''
                 DROP TABLE ProductsInventory;
                ''')
    conn.commit()



def update_inventory(transactionID):
    # needs to check if we need to update log table
    conn = connect_to_db('rubensasson')
    cursor = conn.cursor()
    query = cursor.execute('select * from productsInventory')
    if query.count() == 0:
        for i in range(1, constants.Y):
            if i == 1:
                cursor.execute("INSERT INTO productsInventory(productID, inventory) VALUES (?,?)",
                                (i, constants.COMPLEMENTARY_AMOUNT))
            else:
                cursor.execute("INSERT INTO productsInventory(productID, inventory) VALUES (?,?)",
                               (i, constants.P_AMOUNT))
    else:
        cursor.execute('UPDATE productsInventory SET Inventory = constants.COMPLEMENTARY_AMOUNT Where productID = 1')
        cursor.execute("UPDATE productsInventory SET Inventory = constants.P_AMOUNT Where productID != 1")




if __name__ == '__main__':
    #functionDrop()
    #create_tables()
    manege_transactions.manege_transactions(100)
    conn = connect_to_db("rubensasson")
    personnal_cursor = conn.cursor()
    personnal_cursor.execute('''DELETE FROM Locks WHERE productID=2''')
    personnal_cursor.execute('''DELETE FROM Log WHERE productID=2''')
    #personnal_cursor.execute("INSERT into productsInventory(productID, inventory) VALUES (?,?)", (2, 20))
    conn.commit()


