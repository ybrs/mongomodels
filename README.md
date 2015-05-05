# MongoModels

MongoModels is an object mapper for pymongo.

## Design Goals

    * mongomodels tries to be **simple** and **pragmatic**
    * tries not to get into developers way.

## A quick example
### basic crud

```python
import pymongo
from mongomodels import connections, MongoModel, String, Integer, \
    Column, or_, and_, ValidationError, Boolean, belongs_to

client = pymongo.MongoClient()
connections.add(client.testdb)

class User(MongoModel):
    name =  Column(String, required=True)
    age = Column(Integer)


# now we can create a user
u = User()
u.name = "foobar"
u.age = 11
u.save()

# update the instancce
u.age = 12
u.save()

# now we can query the user
>>> User.query.filter_by(name="foobar").first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359526096>
# or you can use filtering api like sqlalchemy's
>>> User.query.filter(User.age < 13).first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359511888>
>>> User.query.filter(User.age == 12).first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359511824>

# you can combine conditions with or_, and_ etc.
>>> User.query.filter(and_(User.age > 10, User.age <20)).first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359542608>

# you can chain filters
>>> User.query.filter_by(name="foobar").filter(User.age > 10).filter(User.age < 13).first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359572560>

# a query is an iterator
>>> for user in User.query.filter_by(name="foobar"):
...    print user
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4359572560>

# you can side step query interface and use pymongo/mongodb's criterias
>>> User.query.filter({'name':'foobar'}).all()
# also you can chain them
>>> User.query.filter({'name':{'$regex':'^foob'}}).filter(User.age > 10).filter(User.age < 13).first()
<User(age:12 _id:55490785c8bd0c19b76a4d1f name:foobar) object at  4344295568>

# delete the user
>>> u.delete()

```

### relationships

We currently only have one-to-many relationship, and it works like this

```python

class User(MongoModel):
    name = Column(String)

class Child(MongoModel):
    name = Column(String)
    belongs_to(User)

# belongs_to helper will install some magic to User model, like
u = User(name="user 1")
child = Child(name="child 1")
u.children.add(child)

# you can get the first child of the user. as you can see user_id column is added magically.
>>> u.children.first()
<Child(_id:55490958c8bd0c1bf7eba089 user_id:55490958c8bd0c1bf7eba088 name:child 1) object at  4378765648>

# or you can add/combine some queries
>>> u.children.filter_by(name='child 1').first()
<Child(_id:55490a1dc8bd0c1d3bb92625 user_id:55490a1dc8bd0c1d3bb92624 name:child 1) object at  4396620880>

# Child class also gets a helper property
>>> child.user
<User(_id:55490a1dc8bd0c1d3bb92624 name:user 1) object at  4382514768>

```

## Install
for now you can install it with pip from github

```
pip install -U git+https://github.com/ybrs/mongomodels.git
```

## License
This project is licensed with MIT
