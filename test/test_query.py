import unittest
import pymongo
import logging
logging.basicConfig(level=logging.DEBUG)

from mongomodels.column import connections, MongoModel, String, Integer, \
    Column, or_, ValidationError, Boolean, belongs_to

class User(MongoModel):
    name =  Column(String, required=True)
    age = Column(Integer)
    role = Column(String,
                    required=False,
                    validator=lambda val: val in ('user', 'admin') )

class Note(MongoModel):
    belongs_to('project')
    note = Column(String, required=True)

class Project(MongoModel):
    belongs_to(User)
    name = Column(String, required=True)

class TestColumns(unittest.TestCase):

    def setUp(self):
        #
        client = pymongo.MongoClient()
        connections.add(client.testdb)
        # start fresh
        client.testdb.users.remove()
        client.testdb.projects.remove()
        client.testdb.notes.remove()
        client.testdb.categories.remove()

    def test_basic_relationship(self):
        u = User()
        u.age = 15
        u.name = 'foo'
        u.save()
        assert u._id
        assert u.projects.count() == 0
        p = Project(name="foo")
        u.projects.add(p)
        self.assertEqual(u.projects.count(), 1)
        assert p.user_id == u._id
        assert p.user._id == u._id

        note = Note(note="foobar")
        p.notes.add(note)
        assert u.projects.first().notes.first()._id == note._id


    def test_basic_crud(self):
        u = User()
        u.age = 15
        u.name = 'foo'
        u.save()
        assert u._id

        dbuser = User.query.filter_by(age=15).one()
        assert dbuser
        self.assertIsInstance(dbuser, User)
        u.age = 16
        u.save()

        dbuser = User.query.filter_by(age=16).one()
        assert dbuser.age == 16

        u2 = User()
        u2.age = 20
        with self.assertRaises(ValidationError):
            u2.save()

        u2.name = 'f'

        u2.role = 'foobar'
        with self.assertRaises(ValidationError):
            u2.save()

        u2.role = 'user'
        u2.save()


        dbusers = User.query.filter(User.age.in_([16, 20])).all()
        print dbusers
        assert len(dbusers) == 2

        User.query.filter(User.age < 20).delete()

        dbusers = User.query.filter(User.age.in_([16, 20])).all()
        assert len(dbusers) == 1

        u = User(age=24)
        u.name = 'g'
        u.save()

        u = User(age=25)
        u.name = 'foobar'
        u.save()

        dbusers = User.query.filter(or_(
            User.age == 20,
            User.age == 24
        )).all()

        assert len(dbusers) == 2

        # test multi filters
        dbusers = User.query.filter(User.age == 25).filter_by(name='foobar').all()
        assert len(dbusers) == 1
        #
        dbusers = User.query.filter(User.age == 25).filter_by(name='foo').all()
        assert len(dbusers) == 0




if __name__ == '__main__':
    unittest.main()