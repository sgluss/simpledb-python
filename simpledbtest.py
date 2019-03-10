import unittest
import simpledb

class SimpleDBTest(unittest.TestCase):

    def testUnset(self):
        simpleDB = simpledb.SimpleDB()
        simpleDB.Set("test", 10)
        val = simpleDB.Get("test") 
        self.assertEqual(val, 10, "expected value 10")
        simpleDB.Unset("test")
        self.assertRaises(KeyError,simpleDB.Get,"test")

    
    def testRollback(self):
        simpleDB = simpledb.SimpleDB()
        
        simpleDB.Begin()
        simpleDB.Set("test", 10)
        val = simpleDB.Get("test") 
        self.assertEqual(val, 10, "expected value 10")
        
        simpleDB.Begin()
        simpleDB.Set("test",20)
        val = simpleDB.Get("test")
        self.assertEqual(val, 20, "expected value 20")
        
        # TODO should we create a custom error 
        simpleDB.Rollback()
        
        val = simpleDB.Get("test") 
        self.assertEqual(val, 10, "expected value 10")
        
        simpleDB.Rollback()
         
        self.assertRaises(KeyError,simpleDB.Get,"test")


    def testCommit(self): 
        simpleDB = simpledb.SimpleDB()
        
        simpleDB.Begin()
        simpleDB.Set("test", 30)
        
        simpleDB.Begin()
        simpleDB.Set("test", 40)
        
        simpleDB.Commit()
        val = simpleDB.Get("test") 
        self.assertEqual(val, 40, "expected value 40")
        
        simpleDB.Rollback()

    def testTransactionComplex(self):
        simpleDB = simpledb.SimpleDB()
        simpleDB.Set("test", 50)
                 
        simpleDB.Begin()
        
        val = simpleDB.Get("test") 
        self.assertEqual(val, 50, "expected value 50")

        simpleDB.Set("test", 60)
        simpleDB.Begin()
        simpleDB.Unset("test")

        self.assertRaises(KeyError,simpleDB.Get,"test")
        
        simpleDB.Rollback()
        
        val = simpleDB.Get("test") 
        self.assertEqual(val, 60, "expected value 60")

        simpleDB.Commit()
        
        val = simpleDB.Get("test") 
        self.assertEqual(val, 60, "expected value 60")

        simpleDB.Begin()
        simpleDB.Unset("test")

        self.assertRaises(KeyError,simpleDB.Get,"test")
        simpleDB.Rollback()
        
        
        val = simpleDB.Get("test") 
        self.assertEqual(val, 60, "expected value 60")
         
        self.assertRaises(Exception, simpleDB.Commit)            


if __name__ == '__main__':
    unittest.main()
