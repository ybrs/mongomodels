import unittest
import pymongo
import logging
logging.basicConfig(level=logging.DEBUG)

from mongomodels import connections, MongoModel, String, Integer, \
    Column, or_, ValidationError, Boolean, belongs_to, has_and_belongs_to


# a category has many products and a product belongs to more than one category

class Category(MongoModel):
    name = Column(String)

class CategoryProducts(MongoModel):
    category_id = Column(String)
    product_id = Column(String)

class Product(MongoModel):
    has_and_belongs_to(Category, through=CategoryProducts)
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

        assert CategoryProducts.query.count() > 0
        cp = CategoryProducts.query.first()
        assert cp.category_id == c._id
        assert cp.product_id == p._id

        np = c.products.first()
        assert np._id == p._id and isinstance(np, Product)

        c.products.remove(p)
        assert CategoryProducts.query.count() == 0

    def test_many_to_many_add_reverse(self):
        c = Category(name='cat')
        c.save()

        c2 = Category(name='cat 2')
        c2.save()

        p = Product(name='product')
        p.save()

        p.categories.add(c)

        assert CategoryProducts.query.count() > 0
        cp = CategoryProducts.query.first()
        assert cp.category_id == c._id
        assert cp.product_id == p._id

        nc = p.categories.first()
        assert nc._id == c._id and isinstance(nc, Category)

        p.categories.remove(c)
        assert CategoryProducts.query.count() == 0
        nc = p.categories.first()
        assert nc is None


if __name__ == '__main__':
    unittest.main()