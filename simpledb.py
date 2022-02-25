class SimpleDB:
    def __init__(self):
        self.kv_store = {}
        self.transaction = None

    def set(self, key, value):
        """set sets the value associated with the key"""
        if not self.transaction:
            self.kv_store[key] = value
        else:
            # update transaction log
            pass

    def get(self, key):
        """
        get returns the value associated with the key
        get should raise a KeyError if the key doesn't exist
        """
        if key in self.kv_store:
            return self.kv_store[key]

        raise KeyError(f"key {key} not found in store")

    def unset(self, key):
        """unset should delete the key from the db"""
        if not self.transaction:
            if key in self.kv_store:
                self.kv_store.pop(key)
        else:
            # update transaction log
            pass

    def begin(self):
        """begin starts a new transaction"""
        if self.transaction is None:
            self.transaction = []
        else:
            this_transaction = self.transaction
            while isinstance(this_transaction, list):
                if not isinstance(this_transaction[-1], list):
                    this_transaction.append([])
                    return
                this_transaction = this_transaction[-1]

    def commit(self):
        """
        commit commits all transactions
        it should raise an Exception if there is no ongoing transaction
        """
        pass

    def rollback(self):
        """
        rollback undoes the most recent transaction
        it should raise an Exception if there is no ongoing transation
        """
        pass
