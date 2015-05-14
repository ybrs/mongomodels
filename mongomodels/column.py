from bson.objectid import ObjectId as ObjectId_
from .base import relationships_reg, model_registery, connections
import inflection
import logging

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    pass

class ColumnType(object):

    def __init__(self, required=False, validator=None):
        """

        :param required: is this field required when saving
        :param validator: you can pass your own validator function, it takes only one param. value
        :return:
        """
        self.value = None
        self.default_value = None
        self.required = required

        self.validator = validator or self._validate

    def validate(self, value):
        if value is None:
            if not self.required:
                return True
        return self.validator(value)

    def _validate(self, value):
        """
        existing ColumnType classes should override this if they want to do extra validation logic
        we return true for everything

        :param value:
        :return: boolean
        """
        return True

class Double(ColumnType):
    bson_number = 1

class String(ColumnType):
    bson_number = 2

    def _validate(self, value):
        return bool(value)

class Object(ColumnType):
    bson_number = 3

class Array(ColumnType):
    bson_number = 4

class Blob(ColumnType):
    bson_number = 5

class ObjectId(ColumnType):
    bson_number = 7

    def __init__(self, auto=True):
        self.auto = auto
        self.default_value = None
        super(ObjectId, self).__init__()

    def _validate(self, value):
        if self.auto and value is None:
            return True
        return ObjectId_.is_valid(value)

class Boolean(ColumnType):
    bson_number = 8

class Date(ColumnType):
    bson_number = 9

class Null(ColumnType):
    bson_number = 10

class RegularExpression(ColumnType):
    bson_number = 11

class JavaScript(ColumnType):
    bson_number = 13

class Symbol(ColumnType):
    bson_number = 14

class JavaScriptWithScope(ColumnType):
    bson_number = 15

class Integer(ColumnType):
    bson_number = 16

    def validate(self, value):
        return isinstance(value, ( int, long ))

class Timestamp(ColumnType):
    bson_number = 17

class BigInteger(ColumnType):
    bson_number = 18

class Any(ColumnType):
    """this is a special column type that doesnt do any type check, accepts anything you push
    """
    def _validate(self, value):
        return True

class Criteria(object):

    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def column_to_value(self, maybe_column):
        if isinstance(maybe_column, Column):
            return maybe_column.name
        else:
            return maybe_column

    def as_mongo_expression(self):
        left = self.column_to_value(self.left)
        right = self.column_to_value(self.right)
        # TODO: this is for mongo < 3.00
        if self.op == '$eq':
            return {left: right}

        return {left: {self.op: right}}

class NestedCriteria(object):
    def __init__(self, op, args):
        self.op = op
        self.args = args

    def as_mongo_expression(self):
        return {self.op: self.args}


def or_(*args):
    return NestedCriteria('$or', [arg.as_mongo_expression() if isinstance(arg, Criteria) else arg for arg in args])

def and_(*args):
    return NestedCriteria('$and', [arg.as_mongo_expression() if isinstance(arg, Criteria) else arg for arg in args])

def not_(arg):
    return NestedCriteria('$not', arg.as_mongo_expression() if isinstance(arg, Criteria) else arg)

def nor_(*args):
    """$nor performs a logical NOR operation

    See http://docs.mongodb.org/manual/reference/operator/query/nor/#op._S_nor

    :param args:
    :return: NestedCategory
    """
    return NestedCriteria('$nor', [arg.as_mongo_expression() for arg in args])


class Column(object):

    def __init__(self, *args, **kwargs):
        self._column_type = None
        for arg in args:

            if issubclass(arg, ColumnType):
                try:
                    arg = arg(**kwargs)
                except Exception as e:
                    logger.exception("exception while initializing %s" % arg)
                    raise

            if isinstance(arg, ColumnType):
                self._column_type = arg
                self.default_value = arg.default_value

        if self._column_type is None:
            self._column_type = Any()


    def validate(self, value):
        return self._column_type.validate(value)

    def in_(self, other):
        return Criteria(op="$in", left=self, right=other)

    def nin_(self, other):
        return Criteria(op="$nin", left=self, right=other)

    def exists_(self, other=True):
        return Criteria(op="$exists", left=self, right=other)

    def __lt__(self, other):
        return Criteria(op="$lt", left=self, right=other)

    def __le__(self, other):
        return Criteria(op="$lte", left=self, right=other)

    def __eq__(self, other):
        # TODO: if mongo version > 3.0.0
        # return Criteria(op="$eq", left=self, right=other)
        return Criteria(op="$eq", left=self, right=other)

    def __ne__(self, other):
        return Criteria(op="$ne", left=self, right=other)

    def __gt__(self, other):
        return Criteria(op="$gt", left=self, right=other)

    def __ge__(self, other):
        return Criteria(op="$gte", left=self, right=other)

    def mod_(self, other):
        """
        see http://docs.mongodb.org/manual/reference/operator/query/mod/#op._S_mod
        """
        raise Exception('not implemented')

    def regexp(self, other):
        """
        see http://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_regex
        """
        raise Exception('not implemented')

    def type_(self, other):
        """
        see http://docs.mongodb.org/manual/reference/operator/query/type/#op._S_type
        """
        raise Exception('not implemented')

class MongoModelMeta(type):

    def __init__(cls, name, bases, dct):
        if name == 'MongoModel':
            super(MongoModelMeta, cls).__init__(name, bases, dct)
            return

        model_registery[name] = cls

        cls.__columns__ = {}

        if not '__collection__' in cls.__dict__:
            cls.__collection__ = inflection.pluralize(inflection.underscore(cls.__name__))

        cls._scan_columns()

        super(MongoModelMeta, cls).__init__(name, bases, dct)

    def _scan_columns(cls):
        import inspect
        members = dict(inspect.getmembers(cls))
        for k, v in members.iteritems():
            if isinstance(v, Column):
                v.name = k
                cls.__columns__[k] = v

class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()

def process_any_remaining_relationships():
    t = []
    for rel in relationships_reg:
        # are we yet defined
        klass = model_registery.get(rel['called_in_class'])
        if isinstance(rel['other'], str):
            other = model_registery.get(rel['other'].capitalize())
        else:
            other = rel['other']

        if klass and other:
            rel.pop('other')
            RelationshipBelongsTo(klass, other, **rel)
            t.append(rel)

    for r in t:
        relationships_reg.remove(r)

class MongoModel(object):
    __metaclass__ = MongoModelMeta

    _id = Column(ObjectId, auto=True)

    def __init__(self, **kwargs):

        process_any_remaining_relationships()

        for k, v in self.__columns__.iteritems():
            setattr(self, k, getattr(v, 'default_value'))

        self.__connection__ = kwargs.pop('__connection__', connections.get_default())

        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def set_connection(self, connection):
        self.__connection__ = connection
        return self

    def get_connection(self):
        return self.__connection__.pymongo_connection

    def __repr__(self):
        col_repr = []
        for k, v in self.__columns__.iteritems():
            ov = getattr(self, k)
            if len(str(ov)) > 50:
                ov = ov[0:50]
            col_repr.append('%s:%s' % (k, ov))
        return u'<%s(%s) object at  %s>' % (self.__class__.__name__, ' '.join(col_repr), id(self))

    @classproperty
    def query(self):
        """returns a query instance

        :return: Query
        """
        return Query(from_=self)

    @classmethod
    def query_from_connection(self, connection):
        return Query(from_=self, connection=connection)

    def validate(self):
        for k, v in self.__class__.__columns__.iteritems():
            if not v.validate(getattr(self, k)):
                raise ValidationError('validation error on Column: %s - value: %s' % (k, getattr(self, k)))

    def insert(self):
        obj = {}
        for k, v in self.__columns__.iteritems():
            obj[k] = getattr(self, k)
        obj.pop('_id')
        self._id = self.get_connection()[self.__collection__].insert(obj)

    def update(self):
        criteria = {'_id': self._id}
        obj = {}
        for k, v in self.__columns__.iteritems():
            obj[k] = getattr(self, k)
        obj.pop('_id')
        self.get_connection()[self.__collection__].update(criteria, {'$set': obj})

    def delete(self):
        criteria = {'_id': self._id}
        self.get_connection()[self.__collection__].remove(criteria)

    def save(self):
        self.validate()
        if getattr(self, '_id') and not isinstance(getattr(self, '_id'), Column):
            self.update()
        else:
            self.insert()

class Query(object):

    def __init__(self, from_, connection=None):
        self.criterias = []
        self.from_ = from_

        self.limit_ = None
        self.offset_ = None
        self.sort_ = None

        self.connection  = connection
        if self.connection is None:
            self.connection = connections.get_default()

    def filter(self, *criterias):
        for criteria in criterias:
            if isinstance(criteria, dict):
                self.criterias.append(criteria)
            else:
                self.criterias.append(criteria.as_mongo_expression())
        return self

    def filter_by(self, **kwargs):
        for k, v in kwargs.iteritems():
            self.criterias.append(getattr(self.from_, k).__eq__(v).as_mongo_expression())
        return self

    def get_criteria(self):
        compiled = []
        for k in self.criterias:
            compiled.append(k)
        if len(compiled) == 1:
            return compiled[0]

        if len(compiled) == 0:
            return {}

        return {'$and': compiled}

    def get_connection(self):
        return self.connection.pymongo_connection[self.from_.__collection__]

    def sort(self, *args):
        self.sort_ = args
        return self

    def get_cursor(self):
        cursor = self.get_connection().find(self.get_criteria())

        if self.limit_:
            cursor.limit(self.limit_)

        if self.offset_:
            cursor.skip(self.offset_)

        if self.sort_:
            cursor.sort(*self.sort_)

        return cursor

    def limit(self, i):
        self.limit_ = i

    def one(self):
        """
        returns the first instance found in collection, raises exception if
        there is more than one instance or if there is no instance found
        """
        self.limit(2)
        u = self.get_cursor()
        u = list(u)
        assert u, "expected one object"
        if len(u) > 1:
            assert u, "expected one object, more than one received"
        return self.from_(**self.prepare_data(u[0]))

    def prepare_data(self, data):
        data['__connection___'] = self.connection
        return data

    def first(self):
        """
        returns first instance found in the collection, or None
        """
        try:
            data = self.get_cursor()[0]
            return self.from_(**self.prepare_data(data))
        except IndexError:
            return None

    def delete(self):
        return self.get_connection().remove(self.get_criteria())

    def count(self):
        return self.get_cursor().count()

    def __iter__(self):
        for o in self.get_cursor():
            yield self.from_(self.prepare_data(**o))

    def all(self):
        return [self.from_(**v) for v in self.get_cursor()]

class RelationshipHasOne(object):
    """
    this is used in reverse of belongs_to, RelationshipBelongsTo
    """
    def __init__(self, klass, other, rel_column):
        self.klass = klass
        self.other = other
        self.rel_column = rel_column

    def __get__(self, instance, owner):
        return RelationshipHasOneQuery(self.other, instance, self.rel_column).\
            filter({'_id': getattr(instance, self.rel_column)}).first()

class RelationshipBelongsTo(object):
    """
    this is the property added by belongs_to(obj) helper.

    """
    def __init__(self, klass, other, rel_column=None, backref=None, **kwargs):
        self.klass = klass
        self.other = other
        if rel_column:
            prop_name = inflection.pluralize(backref) # phonenumbers
            backref_id = rel_column # owner_id
            backref = backref_id.split('_id')[0] # parent
        else:
            # user.projects
            prop_name = inflection.pluralize(self.klass.__name__).lower()
            # project.user_id
            backref_id = '%s_id' % inflection.singularize(self.other.__name__).lower()
            # project.user
            backref = inflection.singularize(self.other.__name__).lower()

        setattr(other, prop_name, self)
        self.rel_column = backref_id
        klass.__columns__[backref_id] = Column(ObjectId)
        setattr(klass, backref, RelationshipHasOne(klass, other, backref_id))

    def __get__(self, instance, owner):
        return RelationshipQuery(self.klass, instance, self.rel_column).\
            filter({self.rel_column: getattr(instance, '_id')})

class RelationshipHasOneQuery(Query):
    """
    this is used for reverse property for one to many relationship

    eg:
        >>> class Parent(MongoModel):
        >>>    pass

        >>> class Child(MongoModel):
        >>>    belongs_to(Parent)

        >>> c = Child()
        >>> p = Parent()
        >>> p.children.add(c)
        >>> c.parent._id == p._id
        True

    in this example c.parent is RelationshipHasOneQuery,
    its created by RelationshipBelongsTo

    """
    def __init__(self, from_,
                 owner_instance, rel_column):
        self.owner = owner_instance
        self.rel_column = rel_column
        super(RelationshipHasOneQuery, self).__init__(from_=from_)


class RelationshipQuery(Query):
    def __init__(self, from_,
                 owner_instance, rel_column):
        self.owner = owner_instance
        self.rel_column = rel_column
        super(RelationshipQuery, self).__init__(from_=from_)

    def add(self, instance):
        self.owner.save()
        setattr(instance, self.rel_column, getattr(self.owner, '_id'))
        instance.save()

    def remove(self, instance):
        self.owner.save()
        setattr(instance, self.rel_column, None)
        instance.save()


if __name__ == "__main__":

    import pymongo
    client = pymongo.MongoClient()
    connections.add(client.testdb)

    class User(MongoModel):
        age = Column(Integer)

    user = User()
    user.age = 12
    assert user._id is None, ">>> _id should be none"
    user.save()
    #
    user.age = 14
    user.save()

    user = User()
    user.age = 21
    user.save()

    #
    print "-----"
    for u in User.query.filter(User.age < 20):
        print ">>>", u


    User.query.filter(User.age < 20).delete()

    User.query.first()