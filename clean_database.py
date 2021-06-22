import manege_transactions

def clean():
    conn = manege_transactions.connect_to_db('rubensasson')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Locks')
    cursor.execute('DELETE FROM Log')
    cursor.execute('DELETE FROM ProductsOrdered')
    cursor.execute('DELETE FROM ProductsInventory')
    conn.commit()


if __name__ == '__main__':
    clean()

