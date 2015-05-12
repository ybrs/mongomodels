from .base import relationships_reg

def belongs_to(klass_or_name, rel_column=None, backref=None ):
    import inspect
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    called_in_class = calframe[1][3]
    relationships_reg.append({'called_in_class': called_in_class,
                              'relationship': 'belongs_to',
                              'other': klass_or_name,
                              'rel_column': rel_column, 'backref': backref})
    return klass_or_name
