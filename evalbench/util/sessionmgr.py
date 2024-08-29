class SesssionManager:
    def __init__(self):
        self.sessions = {}

    def get_session(self, session_id):
        return self.sessions[session_id]

    def create_session(self, session_id):
        self.sessions[session_id] = {}
