import manege_transactions



if __name__ == '__main__':
    manege_transactions.update_inventory('yosi')
    #manege_transactions.manege_transactions(100)

    ##INSERT###

    #personnal_cursor.execute("INSERT into productsInventory(productID, inventory) VALUES (?,?)", (2, 20))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                            # ('ABC_3', 2, 'Read'))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                            # ('RRR_8', 2, 'Read'))
    #personnal_cursor.execute("INSERT into Locks(transactionID, productID, lockType) VALUES(?,?,?)",
                             #('AAA_7', 2, 'Read'))




