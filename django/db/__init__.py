import inspect
from django.conf import settings
from django.core import signals
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import (ConnectionHandler, ConnectionRouter,
    load_backend, DEFAULT_DB_ALIAS, DatabaseError, IntegrityError)
from collections import namedtuple

__all__ = ('backend', 'connection', 'connections', 'router', 'qualified_name',
    'DatabaseError', 'IntegrityError', 'DEFAULT_DB_ALIAS')


# All table names must be QNames. The db_format argument
# tells if the qualified name is in a format that is ready
# to be used in the database or if the qname needs to be
# converted first.
class QName(namedtuple('QName', ['schema', 'table', 'db_format'])):
    """Represents components for a fully qualified SQL sequence, index,
    or table name.

    How these components are ultimately _composed_ is backend-specific.
    If `model` is not passed in, this implies a raw usage of this name
    and not associated with an existing model class.
    """
    def __new__(cls, schema, table, db_format, model=None):
        tup = tuple.__new__(cls, (schema, table, db_format))
        tup.model = model
        return tup


if DEFAULT_DB_ALIAS not in settings.DATABASES:
    raise ImproperlyConfigured("You must define a '%s' database" % DEFAULT_DB_ALIAS)

connections = ConnectionHandler(settings.DATABASES)

router = ConnectionRouter(settings.DATABASE_ROUTERS)


def qualified_name(model, database=None, **hints):
    "Given a model class or instance, return a qualified name."
    if not inspect.isclass(model):
        hints.setdefault('instance', model)
        model = model.__class__
    qname = model._meta.qualified_name
    schema = router.schema_for_db(model, database, **hints)
    if schema is not None:
        qname = QName(schema, qname.table, qname.db_format, model)
    return qname

# `connection`, `DatabaseError` and `IntegrityError` are convenient aliases
# for backend bits.

# DatabaseWrapper.__init__() takes a dictionary, not a settings module, so
# we manually create the dictionary from the settings, passing only the
# settings that the database backends care about. Note that TIME_ZONE is used
# by the PostgreSQL backends.
# We load all these up for backwards compatibility, you should use
# connections['default'] instead.
class DefaultConnectionProxy(object):
    """
    Proxy for accessing the default DatabaseWrapper object's attributes. If you
    need to access the DatabaseWrapper object itself, use
    connections[DEFAULT_DB_ALIAS] instead.
    """
    def __getattr__(self, item):
        return getattr(connections[DEFAULT_DB_ALIAS], item)

    def __setattr__(self, name, value):
        return setattr(connections[DEFAULT_DB_ALIAS], name, value)

connection = DefaultConnectionProxy()
backend = load_backend(connection.settings_dict['ENGINE'])

# Register an event that closes the database connection
# when a Django request is finished.
def close_connection(**kwargs):
    for conn in connections.all():
        conn.close()
signals.request_finished.connect(close_connection)

# Register an event that resets connection.queries
# when a Django request is started.
def reset_queries(**kwargs):
    for conn in connections.all():
        conn.queries = []
signals.request_started.connect(reset_queries)

# Register an event that rolls back the connections
# when a Django request has an exception.
def _rollback_on_exception(**kwargs):
    from django.db import transaction
    for conn in connections:
        try:
            transaction.rollback_unless_managed(using=conn)
        except DatabaseError:
            pass
signals.got_request_exception.connect(_rollback_on_exception)
