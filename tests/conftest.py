"""Database tests are explicit opt-in; their fixture creates a new isolated DB."""
def pytest_addoption(parser):
    parser.addoption("--database-tests", action="store_true", default=False,
                     help="Create an isolated quaketrack_test_<uuid> PostgreSQL database for integration tests")
