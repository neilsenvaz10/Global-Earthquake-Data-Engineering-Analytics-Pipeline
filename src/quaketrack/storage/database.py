"""Read-only PostgreSQL connectivity check; no schema creation."""
from sqlalchemy import create_engine, text

def verify_database(settings):
    engine = create_engine(settings.database_url, connect_args={"connect_timeout": 5},
                           hide_parameters=True)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()
