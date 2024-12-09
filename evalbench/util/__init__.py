from .progress import printProgressBar
from .loghandler import truncateExecutionOutputs
from .sessionmgr import SessionManager

SESSIONMANAGER = SessionManager()


def get_SessionManager():
    return SESSIONMANAGER
