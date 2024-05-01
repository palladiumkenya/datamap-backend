import logging

from settings import settings

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


from cassandra.cqlengine.connection import register_connection, set_default_connection, dict_factory

KEYSPACE = settings.CASSANDRA_DB
HOSTS = [settings.CASSANDRA_HOST]
CREDENTIAL = {'username': settings.CASSANDRA_USER, 'password': settings.CASSANDRA_PASSWORD}
AUTH_PROVIDER = PlainTextAuthProvider(username=settings.CASSANDRA_USER, password=settings.CASSANDRA_PASSWORD)


def cassandra_session_factory():
    cluster = Cluster(HOSTS, auth_provider=AUTH_PROVIDER)
    session = cluster.connect()

    log.info("Creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE
                    )

    log.info("Setting keyspace...")
    session.set_keyspace(KEYSPACE)

    session.row_factory = dict_factory
    session.execute("USE {}".format(KEYSPACE))

    return session


_session = cassandra_session_factory()
register_connection(str(_session), session=_session)
set_default_connection(str(_session))
