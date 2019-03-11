import unittest
import simpledb


class SimpleDBTest(unittest.TestCase):
    def test_unset(self):
        db = simpledb.SimpleDB()
        db.set("test", 10)
        val = db.get("test")
        self.assertEqual(val, 10)
        db.unset("test")
        self.assertRaises(KeyError, db.get, "test")

    def testRollback(self):
        db = simpledb.SimpleDB()

        db.begin()
        db.set("test", 10)
        val = db.get("test")
        self.assertEqual(val, 10)

        db.begin()
        db.set("test", 20)
        val = db.get("test")
        self.assertEqual(val, 20)

        db.rollback()

        val = db.get("test")
        self.assertEqual(val, 10)

        db.Rollback()

        self.assertRaises(KeyError, db.get, "test")

    def testCommit(self):
        db = simpledb.SimpleDB()

        db.begin()
        db.set("test", 30)

        db.begin()
        db.set("test", 40)

        db.commit()
        val = db.get("test")
        self.assertEqual(val, 40)

        db.rollback()

    def testTransactionComplex(self):
        db = simpledb.SimpleDB()
        db.set("test", 50)

        db.begin()

        val = db.get("test")
        self.assertEqual(val, 50)

        db.set("test", 60)
        db.begin()
        db.unset("test")

        self.assertRaises(KeyError, db.get, "test")

        db.rollback()

        val = db.get("test")
        self.assertEqual(val, 60)

        db.commit()

        val = db.get("test")
        self.assertEqual(val, 60)

        db.begin()
        db.unset("test")

        self.assertRaises(KeyError, db.get, "test")
        db.rollback()

        val = db.get("test")
        self.assertEqual(val, 60)

        self.assertRaises(Exception, db.commit)


if __name__ == "__main__":
    unittest.main()
