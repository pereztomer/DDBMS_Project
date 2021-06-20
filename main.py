import pyodbc
import constants
import manege_transactions
import datetime
from pyspark.sql.functions import asc, current_date
from update_inventory import update_inventory
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


if __name__ == '__main__':
    #update_inventory("gobel")
    manege_transactions.manege_transactions(100)

    ##INSERT###

    #personnal_cursor.execute("INSERT into productsInventory(productID, inventory) VALUES (?,?)", (2, 20))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                            # ('ABC_3', 2, 'Read'))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                            # ('RRR_8', 2, 'Read'))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                             #('AAA_7', 2, 'Read'))




