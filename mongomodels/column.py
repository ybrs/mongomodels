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

    def __repr__(self):
        return '<Criteria %s>' % self.as_mongo_expression()


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

    def exists(self):
        """
        check if the field exists
        """
        return Criteria(op="$exists", left=self, right=True)

    def iexact(self, other):
        """string field exactly matches value (case insensitive)"""

    def contains(self, other):
        """string field contains value"""

    def icontains(self, other):
        """string field contains value (case insensitive)"""

    def startswith(self, other):
        """ string field starts with value """

    def istartswith(self, other):
        """ string field starts with value (case insensitive)
        """
    def endswith(self, other):
        """string field ends with value"""

    def iendswith(self, other):
        """string field ends with value (case insensitive)"""

    def match(self, other):
        """performs an $elemMatch so you can match an entire document within an array """

    def mod_(self, other):
        """
        see http://docs.mongodb.org/manual/reference/operator/query/mod/#op._S_mod
        """
        raise Exception('not implemented')

    def regexp(self, other):
        """
        see http://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_regex
        """
        return Criteria(op="$regexp", left=self, right=other)

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

def create_through_class(left, right, left_column_name, right_column_name):
    name = '%s%s' % (right.__name__, left.__name__)
    through_class = type(name, (MongoModel,), {
        left_column_name: Column(ObjectId),
        right_column_name: Column(ObjectId)
    })
    return through_class

def process_any_remaining_relationships():
    t = []
    for rel in relationships_reg:
        if rel['relationship'] == 'has_and_belongs_to':
            left = model_registery.get(rel['called_in_class'])
            if isinstance(rel['other'], str):
                right = model_registery.get(rel['other'].capitalize())
            else:
                right = rel['other']

            if rel['through']:
                # just sanity check
                through_class = rel['through']
                left_id_col = getattr(through_class, '%s_id' % inflection.singularize(left.__name__).lower())
                right_id_col = getattr(through_class, '%s_id' % inflection.singularize(right.__name__).lower())
                RelationshipHasAndBelongsTo(left, right,
                                            through=through_class,
                                            left_id_column=left_id_col, right_id_column=right_id_col)

                # now we swap and add another relationship
                RelationshipHasAndBelongsTo(right, left,
                                            through=through_class,
                                            left_id_column=right_id_col,
                                            right_id_column=left_id_col)

                t.append(rel)
            else:
                # we now create a through class
                left_id_col_name = '%s_id' % inflection.singularize(left.__name__).lower()
                right_id_col_name ='%s_id' % inflection.singularize(right.__name__).lower()

                through_class = create_through_class(left, right, left_id_col_name, right_id_col_name)

                left_id_col = getattr(through_class, '%s_id' % inflection.singularize(left.__name__).lower())
                right_id_col = getattr(through_class, '%s_id' % inflection.singularize(right.__name__).lower())


                RelationshipHasAndBelongsTo(left, right,
                                            through=through_class,
                                            left_id_column=left_id_col, right_id_column=right_id_col)

                # now we swap and add another relationship
                RelationshipHasAndBelongsTo(right, left,
                                            through=through_class,
                                            left_id_column=right_id_col,
                                            right_id_column=left_id_col)

                t.append(rel)

        elif rel['relationship'] == 'belongs_to':
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
        else:
            raise Exception('unknown relationship type - %s' % rel['relationship'])

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
        for k in sorted(self.__columns__.keys()):
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

    @classmethod
    def get_by_id(cls, object_id):
        from bson import json_util, ObjectId as BsonObjectId

        return cls.query.filter_by(_id= BsonObjectId(object_id)).first()

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
        t = []
        for arg in args:
            if isinstance(arg, Column):
                t.append(arg.name)
            else:
                t.append(arg)

        self.sort_ = t
        print self.sort_
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
            yield self.from_(**self.prepare_data(o))

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

class RelationshipHasAndBelongsTo(object):
    """
    this is the property added by belongs_to(obj) helper.

    """
    def __init__(self, left, right, through, left_id_column, right_id_column, **kwargs):
        self.left = left
        self.right = right
        self.through = through
        self.left_id_column = left_id_column
        self.right_id_column = right_id_column

        # add getter property to left
        # user.projects
        prop_name = inflection.pluralize(self.left.__name__).lower()
        setattr(right, prop_name, self)

        # add getter property to right


    def __get__(self, instance, owner):
        """
        owner is the class Category
        instance is category (Category instance)
        """
        return RelationshipQueryThrough(from_=self.left,
                                        through=self.through,
                                         owner_instance=instance,
                                         left_rel_column=self.left_id_column,
                                         right_rel_column=self.right_id_column)


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
        column = Column(ObjectId)
        column.name = backref_id
        klass.__columns__[backref_id] = column
        setattr(klass, backref_id, column)
        setattr(klass, backref, RelationshipHasOne(klass, other, backref_id))
        print ">>>", backref


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

class RelationshipQueryThrough(Query):
    def __init__(self, from_, through,
                 owner_instance, left_rel_column, right_rel_column):
        """

        :param from_: query from class eg: User
        :param through: query through class eg: UserProduct
        :param owner_instance: query attached to instance eg: user
        :param left_rel_column: eg: Column(user_id)
        :param right_rel_column: eg: Column(product_id)
        """

        self.owner = owner_instance
        self.left_rel_column = left_rel_column
        self.right_rel_column = right_rel_column
        self.through = through

        super(RelationshipQueryThrough, self).__init__(from_=from_)


    def add(self, instance):
        """

        find through instance eg:

        user.add(product)

        creates a record in
            UserProduct - user_id: 1, product_id: 1

        :param instance: the instance to be added eg: product
        :return: owner instance
        """
        self.owner.save()
        if not instance._id:
            instance.save()

        through_record = self.through.query.filter({
            self.left_rel_column.name: self.owner._id,
            self.right_rel_column.name: instance._id
        }).first()

        if not through_record:
            through_record = self.through()
            setattr(through_record, self.right_rel_column.name, self.owner._id)
            setattr(through_record, self.left_rel_column.name, instance._id)
            through_record.save()

        setattr(instance, self.left_rel_column.name, getattr(self.owner, '_id'))
        setattr(instance, self.right_rel_column.name, getattr(instance, '_id'))
        instance.save()

        return self.owner

    def remove(self, instance):
        self.owner.save()
        if not instance._id:
            instance.save()

        through_record = self.through.query.filter({
            self.left_rel_column.name: instance._id,
            self.right_rel_column.name: self.owner._id
        }).first()

        if through_record:
            through_record.delete()

    def __iter__(self):
        for t in Query(from_=self.through).filter({self.right_rel_column.name: self.owner._id}):
            left_id = getattr(t, self.left_rel_column.name)
            yield Query(from_=self.from_).filter({'_id': left_id}).first()

    def first(self):
        for i in self:
            return i

    def delete(self):
        raise Exception('Not implemented')

    def count(self):
        return Query(from_=self.through)\
            .filter({self.right_rel_column.name: self.owner._id}).count()

    def all(self):
        return list(self)


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