class Connection(object):
    """
    just a simple wrapper around pymongo connection
    """
    def __init__(self, pymongo_connection):
        self.pymongo_connection = pymongo_connection

    def query(self, model_class):
        return model_class.query_from_connection(self)


class Connections(object):

    _connections = {}

    def get_default(self):
        return self._connections.get('default', None)

    def add_(self, name, pymongo_connection):
        self._connections[name] = Connection(pymongo_connection)
        return self._connections[name]

    def add(self, *args):
        """

        :param args:
        :rtype: Connection
        :return:
        """
        if len(args) == 1:
            return self.add_('default', args[0])
        else:
            return self.add_(*args)
