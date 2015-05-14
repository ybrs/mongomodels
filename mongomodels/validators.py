"""
simple conditions to include in validations

you can use these like,

class Activity(MongoModel):
    activity_kind = Column(String, required=True, validator=notnull and in_('count', 'timely'))


"""

def in_(*args):
    def inner(x):
        return x in args
    return inner

def notnull(col_val):
    return col_val is not None