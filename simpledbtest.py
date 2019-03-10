import unittest
import simpledb

class SimpleDBTest(unittest.TestCase):

    def test_unset(self):
        simpleDB = simpledb.SimpleDB()
        simpleDB.set("test", 10)
        val = simpleDB.get("test") 
        self.assertEqual(val, 10)
        simpleDB.unset("test")
        self.assertRaises(KeyError,simpleDB.get,"test")

    
    def testRollback(self):
        simpleDB = simpledb.SimpleDB()
        
        simpleDB.begin()
        simpleDB.set("test", 10)
        val = simpleDB.get("test") 
        self.assertEqual(val, 10)
        
        simpleDB.begin()
        simpleDB.set("test",20)
        val = simpleDB.get("test")
        self.assertEqual(val, 20)
        
        # TODO should we create a custom error 
        simpleDB.rollback()
        
        val = simpleDB.get("test") 
        self.assertEqual(val, 10)
        
        simpleDB.Rollback()
         
        self.assertRaises(KeyError,simpleDB.get,"test")


    def testCommit(self): 
        simpleDB = simpledb.SimpleDB()
        
        simpleDB.begin()
        simpleDB.set("test", 30)
        
        simpleDB.begin()
        simpleDB.set("test", 40)
        
        simpleDB.commit()
        val = simpleDB.get("test") 
        self.assertEqual(val, 40)
        
        simpleDB.rollback()

    def testTransactionComplex(self):
        simpleDB = simpledb.SimpleDB()
        simpleDB.set("test", 50)
                 
        simpleDB.begin()
        
        val = simpleDB.get("test") 
        self.assertEqual(val, 50)

        simpleDB.set("test", 60)
        simpleDB.begin()
        simpleDB.unset("test")

        self.assertRaises(KeyError,simpleDB.get,"test")
        
        simpleDB.rollback()
        
        val = simpleDB.get("test") 
        self.assertEqual(val, 60)

        simpleDB.commit()
        
        val = simpleDB.get("test") 
        self.assertEqual(val, 60)

        simpleDB.begin()
        simpleDB.unset("test")

        self.assertRaises(KeyError,simpleDB.get,"test")
        simpleDB.rollback()
        
        
        val = simpleDB.get("test") 
        self.assertEqual(val, 60)
         
        self.assertRaises(Exception, simpleDB.commit)            


if __name__ == '__main__':
    unittest.main()
