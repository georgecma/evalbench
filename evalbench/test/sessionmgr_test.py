import pytest
from util.sessionmgr import SessionManager
import time


@pytest.fixture(scope="session")
def manager():
    print("creating sessionmanager")
    sesssionmanager = SessionManager()
    yield sesssionmanager
    print("shutting down sessionmanager")
    sesssionmanager.shutdown()


class TestSessionManager:

    def test_create_session(self, manager):
        session = manager.create_session(f"test_session")
        session_count = len(manager.get_sessions())
        assert session_count == 1

    def test_delete_session(self, manager):
        session = manager.get_session(f"test_session")
        session_count = len(manager.get_sessions())
        assert session_count == 1
        manager.delete_session(f"test_session")
        session_count = len(manager.get_sessions())
        assert session_count == 0

    def test_expire_session(self, manager):
        session = manager.create_session(f"test_session_expire")
        session_count = len(manager.get_sessions())
        assert session_count == 1
        time.sleep(20)
        session_count = len(manager.get_sessions())
        assert session_count == 0
