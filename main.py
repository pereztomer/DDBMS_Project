import pyodbc
from manege_transactions import manege_transactions

server = 'technionddscourse.database.windows.net'
database = 'rubensasson'
username = 'rubensasson'
password = 'Qwerty12!'
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=' + server + ';'
    'DATABASE=' + database + ';'
    'UID=' + username + ';'
    'PWD=' + password+';')

cursor = conn.cursor()

if __name__ == '__main__':
    manege_transactions(10)