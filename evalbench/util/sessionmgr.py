from threading import Thread
import logging
import time
from absl import app
import uuid


class SessionManager:
    def __init__(self):
        self.running = True
        self.sessions = {}
        logging.info("Starting reaper...")
        reaper = Thread(target=self.reaper, args=[])
        reaper.start()

    def get_session(self, session_id):
        return self.sessions[session_id]

    def create_session(self, session_id):
        self.sessions[session_id] = {'create_ts': time.time(), 'session_id': session_id}
        return self.sessions[session_id]

    def get_sessions(self):
        return self.sessions

    def delete_session(self, session_id):
        del self.sessions[session_id]

    def shutdown(self):
        self.running = False

    def reaper(self):
        old_sessions = []
        while self.running:
            logging.info(f"Reaper cycle: {len(self.sessions)}")
            for session_id in self.sessions.keys():
                if time.time() - self.sessions[session_id]['create_ts'] > 10:
                    old_sessions.append(session_id)
            for session_id in old_sessions:
                logging.info(f"Delete session {session_id}.")
                self.delete_session(session_id)
                old_sessions.remove(session_id)
            time.sleep(1)
