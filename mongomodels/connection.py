class Connections(object):
    _connections = {}

    def get_default(self):
        return self._connections['default']

    def add_(self, name, connection):
        self._connections[name] = connection

    def add(self, *args):
        if len(args) == 1:
            self.add_('default', args[0])
        else:
            self.add_(*args)
