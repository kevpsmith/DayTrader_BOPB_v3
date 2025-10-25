import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
import time
import pytz


class Clock:
    """
    A timezone-aware clock abstraction that supports both live and replay modes.

    Modes:
      - 'live': returns real current time and sleeps in real time.
      - 'replay': simulates time by advancing an internal pointer without real delays.
    """
    def __init__(self, mode: str = 'live', start_time: datetime = None):
        """
        :param mode: 'live' or 'replay'
        :param start_time: initial datetime for replay mode (must be timezone-aware)
        """
        self.mode = mode
        self.start_time = start_time
        if self.mode not in {'live', 'replay'}:
            raise ValueError("mode must be 'live' or 'replay'")

        # Time pointer for replay mode
        if self.mode == 'replay':
            if start_time is None:
                raise ValueError("start_time must be provided in replay mode")
            if start_time.tzinfo is None:
                raise ValueError("start_time must be timezone-aware")
            self._current_time = start_time

        # Timezone definitions
        self._utc_tz = pytz.UTC
        self._ny_tz = pytz.timezone('America/New_York')

    def set(self, new_time):
        # only in replay mode, and only move forward
        if self.mode != 'replay':
            raise RuntimeError("Can't set time in live mode")
        if new_time > self._current_time:
            self._current_time = new_time
            
    def now_utc(self) -> datetime:
        """
        Return the current UTC time.
        """
        if self.mode == 'live':
            return datetime.now(self._utc_tz)
        else:
            return self._current_time.astimezone(self._utc_tz)

    def now_ny(self) -> datetime:
        """
        Return the current time in America/New_York.
        """
        if self.mode == 'live':
            return datetime.now(self._ny_tz)
        else:
            return self._current_time.astimezone(self._ny_tz)

    def sleep(self, dt: timedelta):
        """
        Pause execution for the given timedelta.

        - In live mode: real sleep.
        - In replay mode: advance internal clock without sleeping.
        """
        if dt.total_seconds() < 0:
            raise ValueError("sleep duration must be non-negative")

        if self.mode == 'live':
            time.sleep(dt.total_seconds())
        else:
            self._current_time += dt
    
    @staticmethod
    def get_unix_timestamp_et(date: str, time: str, ny_tz=pytz.timezone("America/New_York")) -> int:
        """
        Converts a date and time string (ET) to a UNIX timestamp in milliseconds.
        """
        dt = datetime.strptime(f"{date}T{time}", "%Y-%m-%dT%H:%M:%S")
        dt_et = ny_tz.localize(dt)
        return int(dt_et.timestamp() * 1000)