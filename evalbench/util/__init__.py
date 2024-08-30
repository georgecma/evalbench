from .progress import printProgressBar
from .sessionmgr import SessionManager

SESSIONMANAGER = SessionManager()


def get_SessionManager():
    return SESSIONMANAGER
