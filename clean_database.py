import constants
import manege_transactions
import pyodbc
import time
import main
import constants
from update_inventory import update_inventory

def clean():
    conn = main.connect_to_db('rubensasson')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Locks')
    cursor.execute('DELETE FROM Log')
    cursor.execute('DELETE FROM ProductsOrdered')
    cursor.execute('DELETE FROM ProductsInventory')
    conn.commit()


if __name__ == '__main__':
    clean()
    #update_inventory("gobel")

