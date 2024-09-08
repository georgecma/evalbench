import logging
import queue


class StreamingLogHandler(logging.Handler):
    def __init__(self):
      super().__init__()
      self.records = queue.Queue()

    def emit(self, record):
      self.records.put(record)

    def get_messages(self):
        return self.records

    def close(self):
        self.records.join()
