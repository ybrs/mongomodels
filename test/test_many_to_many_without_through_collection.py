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

class TestManyToMany(unittest.TestCase):

    def setUp(self):
        #
        client = pymongo.MongoClient()
        connections.add(client.testdb)
        # start fresh
        client.testdb.categories.remove()
        client.testdb.products.remove()
        client.testdb.category_products.remove()


    def test_many_to_many_add(self):
        c = Category(name='cat')
        c.save()

        c2 = Category(name='cat 2')
        c2.save()

        p = Product(name='product')
        c.products.add(p)

        np = c.products.first()
        assert np._id == p._id and isinstance(np, Product)

        assert len(list(c.products)) > 0
        c.products.remove(p)
        assert len(list(c.products)) == 0

    def test_many_to_many_add_reverse(self):
        c = Category(name='cat')
        c.save()

        c2 = Category(name='cat 2')
        c2.save()

        p = Product(name='product')
        p.save()

        p.categories.add(c)

        nc = p.categories.first()
        assert nc._id == c._id and isinstance(nc, Category)

        assert len(list(p.categories)) > 0
        p.categories.remove(c)
        assert len(list(p.categories)) == 0


if __name__ == '__main__':
    unittest.main()