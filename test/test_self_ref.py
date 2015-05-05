import unittest
import pymongo
import logging
logging.basicConfig(level=logging.DEBUG)

from mongomodels.column import connections, MongoModel, String, Integer, \
    Column, or_, ValidationError, Boolean, belongs_to

class Category(MongoModel):
    name =  Column(String, required=True)
    belongs_to('category', rel_column='parent_id', backref="children")

class TestColumns(unittest.TestCase):

    def setUp(self):
        #
        client = pymongo.MongoClient()
        connections.add(client.testdb)
        # start fresh
        client.testdb.users.remove()
        client.testdb.phones.remove()

    def test_self_ref(self):
        parent = Category(name='parent')
        child = Category(name='child')
        parent.children.add(child)
        assert parent.children.first()._id == child._id
        assert child.parent._id == parent._id
        parent.children.remove(child)

if __name__ == '__main__':
    unittest.main()