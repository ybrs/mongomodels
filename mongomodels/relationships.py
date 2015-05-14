from .base import relationships_reg
import inspect

def belongs_to(klass_or_name, rel_column=None, backref=None ):
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    called_in_class = calframe[1][3]
    relationships_reg.append({'called_in_class': called_in_class,
                              'relationship': 'belongs_to',
                              'other': klass_or_name,
                              'rel_column': rel_column, 'backref': backref})
    return klass_or_name


def has_and_belongs_to(klass_or_name, rel_column=None, backref=None, through=None):
    """
    this creates an intermediate class and attaches that
    :param klass_or_name:
    :param rel_column:
    :param backref:
    :param through:
    :return:
    """
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    called_in_class = calframe[1][3]
    relationships_reg.append({'called_in_class': called_in_class,
                              'relationship': 'has_and_belongs_to',
                              'other': klass_or_name,
                              'through': through,
                              'rel_column': rel_column, 'backref': backref})
    return klass_or_name
