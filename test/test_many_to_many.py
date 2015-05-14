import unittest
import pymongo
import logging
logging.basicConfig(level=logging.DEBUG)

from mongomodels import connections, MongoModel, String, Integer, \
    Column, or_, ValidationError, Boolean, belongs_to, has_and_belongs_to


# a category has many products and a product belongs to more than one category

class Category(MongoModel):
    name = Column(String)

class Product(MongoModel):
    has_and_belongs_to(Category)
    name = Column(String)

class TestColumns(unittest.TestCase):

    def setUp(self):
        #
        client = pymongo.MongoClient()
        connections.add(client.testdb)
        # start fresh
        client.testdb.students.remove()
        client.testdb.classes.remove()

    def test_many_to_many(self):
        pass


if __name__ == '__main__':
    unittest.main()