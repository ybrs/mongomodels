import unittest
import pymongo
import logging
logging.basicConfig(level=logging.DEBUG)

from mongomodels import connections, MongoModel, String, Integer, \
    Column, or_, ValidationError, Boolean, belongs_to

class User(MongoModel):
    name =  Column(String, required=True)

class Phone(MongoModel):
    belongs_to('user', rel_column='owner_id', backref="phonenumber")
    number = Column(String)

class TestColumns(unittest.TestCase):

    def setUp(self):
        #
        client = pymongo.MongoClient()
        connections.add(client.testdb)
        # start fresh
        client.testdb.users.remove()
        client.testdb.phones.remove()

    def test_relation(self):
        user = User(name="foo")
        user.save()
        p = Phone(number="123456")
        user.phonenumbers.add(p)
        assert user.phonenumbers.first()._id == p._id
        assert p.owner_id == user._id
        assert p.owner._id == user._id

if __name__ == '__main__':
    unittest.main()