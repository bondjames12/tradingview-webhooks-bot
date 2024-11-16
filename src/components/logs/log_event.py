from datetime import datetime

from commons import LOG_LOCATION, LOG_LIMIT

import logging

class LogEvent:
    def __init__(self, parent=None, event_type=None, event_time=None, event_data=None):
        self.parent = parent
        self.event_type = event_type
        self.event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.event_data = event_data.replace(',', ' ') if event_data else None

    def __str__(self):
        return "Event Type: " + self.event_type + " Event Time: " + self.event_time + " Event Data: " + self.event_data

    def get_event_type(self):
        return self.event_type

    def get_event_time(self):
        return self.event_time

    def get_event_data(self):
        return self.event_data

    def set_event_type(self, event_type):
        self.event_type = event_type

    def set_event_time(self, event_time):
        self.event_time = event_time

    def set_event_data(self, event_data):
        self.event_data = event_data

    def to_line(self):
        return self.parent + "," + self.event_type + "," + self.event_time + "," + self.event_data + "\n"

    def as_json(self):
        return {
            "parent": self.parent,
            "event_type": self.event_type,
            "event_time": self.event_time,
            "event_data": self.event_data.strip()
        }

    def from_line(self, line):
        try:
            self.parent, self.event_type, self.event_time, self.event_data = line.split(
                ","
            )
            self.event_time = datetime.strptime(self.event_time, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logging.error(f"ValueError: {e} occurred for line: {line}")
            # Optionally, you can re-raise the exception if you want the program to stop on errors
            raise
        return self

    def write(self):
        try:
            # Opening the file in append mode or write mode based on log limit
            mode = "w" if len(open(LOG_LOCATION, "r").readlines()) >= LOG_LIMIT else "a"
            with open(LOG_LOCATION, mode) as log_file:
                # Acquiring an exclusive lock
                fcntl.flock(log_file, fcntl.LOCK_EX)

                if mode == "w":
                    rows = open(LOG_LOCATION, "r").readlines()
                    log_file.writelines(rows[1:])
                log_file.write(self.to_line())

                # Releasing the lock
                fcntl.flock(log_file, fcntl.LOCK_UN)
        except IOError as e:
            logging.error(f"I/O error({e.errno}): {e.strerror}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
