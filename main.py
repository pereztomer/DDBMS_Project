import pyodbc
import constants
import manege_transactions

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
    conn = connect_to_db()
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
                          transactionID integer,
                          productID integer,
                          action varchar(10),
                          record varchar(2500),
                          check(action='insert' or action='delete' or action='update' or action='read'),
                          foreign key (productID) references ProductsInventory(productID),
                          check(relation='ProductsInventory' or relation='Locks' or relation='ProductsOrdered')
                      );''')
    cursor.execute('''
                      Create table Locks(
                          transactionID integer,
                          productID integer,
                          lockType varchar(10),
                          foreign key(transactionID) references ProductsOrdered(transactionID),
                          foreign key(productID) references ProductsInventory(productID)
                      );''')

    conn.commit()


def functionDrop():
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute('''
                 DROP TABLE Log;
                ''')
    cursor.execute('''
                 DROP TABLE ProductsOrdered;
                ''')

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
    manege_transactions.manege_transactions(100)



