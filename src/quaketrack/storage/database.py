"""Read-only PostgreSQL connectivity check; no schema creation."""
from sqlalchemy import create_engine, text


def create_database_engine(settings):
    """Explicit DB operations only; never creates a schema on connection."""
    return create_engine(settings.database_url, hide_parameters=True,
                         connect_args={"connect_timeout": 5,
                                       "options": "-c timezone=UTC -c statement_timeout=60000 -c lock_timeout=10000"})


def database_identity(engine):
    with engine.connect() as connection:
        return dict(connection.execute(text(
            "SELECT current_database() AS database, current_user AS username, "
            "inet_server_port() AS server_port, version() AS version")).mappings().one())

def verify_database(settings):
    engine = create_engine(settings.database_url, connect_args={"connect_timeout": 5},
                           hide_parameters=True)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()
