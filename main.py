import pyodbc


def connect_to_db() -> pyodbc.Cursor:
    server = 'technionddscourse.database.windows.net'
    database = 'rubensasson'
    username = 'rubensasson'
    password = 'Qwerty12!'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return conn

def connect_to_globalDB() -> pyodbc.Cursor:
    server = 'technionddscourse.database.windows.net'
    database = 'dbteam'
    username = 'dbteam'
    password = 'Qwerty12!'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
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

#cursor.execute()





# cursor.execute('''
#                 insert into ProductsOrdered(transactionID,productsID,amount)
#                 values
#                 ('computer',1,1000),
#                 ('bikini',69, 123),
#                 ('jahnun',12, 12000)
#                 ''')
# cnxn.commit()'''


#cursor.execute('select * from ProductsOrdered')
#for row in cursor:
    #print(row)

if __name__ == '__main__':
    #functionDrop()
    #conn = connect_to_globalDB()
    conn = connect_to_db()
    cursor = conn.cursor()
    #cursor.execute('select * from categoriesToSites')
    #for row in cursor:
        #print(row)
    create_tables()
    cursor.execute('''
                  insert into ProductsOrdered(transactionID,productsID,amount)
                     values
                     ('computer',1,1000)
                     ''')
    cursor.execute('''
                      insert into ProductsInventory(productID,inventory)
                         values
                         (1,30)
                         ''')
    cursor.execute('''
                      insert into Log(timestamp,relation,transactionID, productID,action,record)
                         values
                         (current_date,'ProductsOrdered',1,1,'read','Select * from ProductsOrdered')
                         ''')
    conn.commit()
    cursor.execute('select * from ProductsOrdered')
    for row in cursor:
        print(row)
    cursor.execute('select * from ProductsInventory')
    for row in cursor:
        print(row)
    cursor.execute('select * from Log')
    for row in cursor:
        print(row)
