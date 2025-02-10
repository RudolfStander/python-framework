from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Tuple

from dateutil.parser import parse
from dateutil.tz import gettz

TIME_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%f-%Z"
MONTH_DAYS = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


class TIMEZONES(Enum):
    UTC = gettz("UTC")
    SAST = gettz("Africa/Johannesburg")


def sastnow() -> datetime:
    return utc_datetime_to_zone_datetime(datetime.utcnow(), TIMEZONES.SAST.value)


def utc_timestamp_to_zoned_timestamp(timestamp_str: str, zone: datetime.tzinfo) -> str:
    try:
        timestamp = parse(timestamp_str)
        return utc_datetime_to_zone_datetime(timestamp, zone).strftime(
            TIME_FORMAT_STRING
        )
    except:
        print(
            "ERROR - [time.utc_timestamp_to_zoned_timestamp] Failed to transform timestamp. Defaulting to passed in format: ",
            timestamp_str,
        )
        return timestamp_str


def datetime_to_zoned_timestamp(timestamp: datetime, zone: datetime.tzinfo) -> str:
    try:
        return utc_datetime_to_zone_datetime(timestamp, zone).strftime(
            TIME_FORMAT_STRING
        )
    except:
        print(
            "ERROR - [time.datetime_to_zoned_timestamp] Failed to convert datetime to zoned timestamp string. Defaulting to non zoned: ",
            string_from_date(timestamp),
        )
        return string_from_date(timestamp)


def utc_datetime_to_zone_datetime(
    timestamp: datetime, zone: datetime.tzinfo
) -> datetime:
    try:
        datetime_utc = timestamp.replace(tzinfo=TIMEZONES.UTC.value)
        return datetime_utc.astimezone(zone)
    except:
        print(
            "ERROR - [time.utc_datetime_to_zone_datetime] Failed to transform datetime. Defaulting to passed in datetime."
        )
        return timestamp


def timestamp_to_utc_timestamp(timestamp_str: str):
    try:
        date = parse(timestamp_str)
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        print(
            "ERROR - [time.timestamp_to_utc_timestamp] Failed to transform timestamp. Defaulting to passed in format: ",
            timestamp_str,
        )
        return timestamp_str


def utc_now():
    try:
        return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    except:
        print("ERROR - [time.utc_now] Failed to create UTC NOW string.")
        return ""


def utc_now_datetime():
    return datetime.utcnow()


def to_utc_timestamp(timestamp_str: str):
    try:
        return (
            datetime.fromisoformat(timestamp_str).isoformat(timespec="milliseconds")
            + "Z"
        )
    except:
        print(
            "ERROR - [time.to_utc_timestamp] Failed to transform timestamp. Defaulting to passed in format: ",
            timestamp_str,
        )
        return timestamp_str


def date_from_float(timestamp_float: float) -> datetime:
    try:
        return datetime.fromtimestamp(timestamp_float)
    except:
        print("ERROR - [time.date_from_float] Failed to transform timestamp")
        return None


def date_from_string(timestamp_str: str) -> datetime:
    try:
        return parse(timestamp_str)
    except:
        print("ERROR - [time.date_from_string] Failed to transform timestamp")
        return None


def string_from_date(date):
    try:
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        print("ERROR - [time.string_from_date] Failed to create UTC string.")
        return None


def unzoned_utc_string_from_zoned_date(date):
    try:
        date = date.astimezone(TIMEZONES.UTC.value)
        return date.isoformat(timespec="milliseconds")
    except:
        print(
            "ERROR - [time.unzoned_utc_string_from_zoned_date] Failed to create unzoned UTC string."
        )
        return None


def _parse_delta(
    delta_range: str, current_month: int = 1, current_day: int = 1
) -> Tuple[str, timedelta]:
    timespec = "s"
    direction = "PLUS"
    delta_amount: int = None
    delta: timedelta = None

    if delta_range[-1] not in ["M", "d", "h", "m", "s"]:
        raise Exception(
            "ERROR - [time.is_date_in_range_from_now] range timespec invalid [%s]. Must be one of [M, d, h, m, s]"
            % delta_range[-1]
        )
    else:
        timespec = delta_range[-1]
        delta_range = delta_range[0:-1]

    if delta_range[0] not in ["-", "+"] and not delta_range[0].isdigit():
        raise Exception(
            "ERROR - [time.is_date_in_range_from_now] range direction invalid [%s]. Must be one of [+, -] or blank"
            % delta_range[0]
        )
    elif delta_range[0] == "-":
        direction = "MINUS"
        delta_range = delta_range[1:]
    elif delta_range[0] == "+":
        direction = "PLUS"
        delta_range = delta_range[1:]

    try:
        delta_amount = int(delta_range)
    except:
        raise Exception(
            "ERROR - [time.is_date_in_range_from_now] range time invalid [%s]. Must only contain digits"
            % delta_range
        )

    if timespec == "s":
        delta = timedelta(seconds=delta_amount)
    elif timespec == "m":
        delta = timedelta(minutes=delta_amount)
    elif timespec == "h":
        delta = timedelta(hours=delta_amount)
    elif timespec == "d":
        delta = timedelta(days=delta_amount)
    elif timespec == "M":
        # subtract or add 1 month from current_month and handle under/overflow
        days_in_delta = 0

        if direction == "PLUS":
            next_month = current_month

            for i in range(delta_amount):
                # add days in current month to days_in_delta
                days_in_delta += MONTH_DAYS[next_month]
                next_month += 1

                # handle overflow
                if next_month > 12:
                    next_month = 1

        elif direction == "MINUS":
            previous_month = current_month

            for i in range(delta_amount):
                previous_month -= 1

                # handle underflow
                if previous_month < 1:
                    previous_month = 12

                # add days in previous month to days_in_delta
                days_in_delta += MONTH_DAYS[previous_month]

        delta = timedelta(days=days_in_delta)

    return direction, delta


def datetime_delta(timestamp: datetime, delta_range: str) -> datetime:
    direction, delta = _parse_delta(delta_range, timestamp.month, timestamp.day)

    return timestamp + delta if direction == "PLUS" else timestamp - delta


def now_delta(delta_range: str):
    date_now = utc_now_datetime()

    return datetime_delta(date_now, delta_range)


def is_date_in_range_from_now(timestamp_start: str, delta_range: str):
    checked_date = date_from_string(timestamp_to_utc_timestamp(timestamp_start))
    direction, delta = _parse_delta(delta_range, checked_date.month, checked_date.day)

    if checked_date is None:
        print(
            "ERROR - [time.is_date_in_range_from_now] could not parse timestamp_start [%s]"
            % timestamp_start
        )
        return False

    date_now = date_from_string(utc_now())
    date_end = date_now + delta if direction == "PLUS" else date_now - delta

    if direction == "PLUS":
        return checked_date >= date_now and checked_date <= date_end
    else:
        return checked_date >= date_end and checked_date <= date_now


def date_diff(date_start: str, date_end: str, in_microseconds=False):
    date_start_sanitised = date_from_string(timestamp_to_utc_timestamp(date_start))
    date_end_sanitised = date_from_string(timestamp_to_utc_timestamp(date_end))

    delta = date_end_sanitised - date_start_sanitised

    if in_microseconds:
        return delta / timedelta(microseconds=1)

    return str(delta)


def set_time(
    date, hours: int = 0, minutes: int = 0, seconds: int = 0, microseconds: int = 0
) -> datetime:
    date_copy: datetime = deepcopy(date)
    date_copy = date_copy.replace(hour=hours)
    date_copy = date_copy.replace(minute=minutes)
    date_copy = date_copy.replace(second=seconds)
    date_copy = date_copy.replace(microsecond=microseconds)
    return date_copy


def is_before(date_1: str, date_2: str):
    sanitised_date_1 = date_from_string(timestamp_to_utc_timestamp(date_1))
    sanitised_date_2 = date_from_string(timestamp_to_utc_timestamp(date_2))

    return sanitised_date_1 < sanitised_date_2


def is_after(date_1: str, date_2: str):
    sanitised_date_1 = date_from_string(timestamp_to_utc_timestamp(date_1))
    sanitised_date_2 = date_from_string(timestamp_to_utc_timestamp(date_2))

    return sanitised_date_1 > sanitised_date_2


def equals(date_1: str, date_2: str):
    sanitised_date_1 = date_from_string(timestamp_to_utc_timestamp(date_1))
    sanitised_date_2 = date_from_string(timestamp_to_utc_timestamp(date_2))

    return sanitised_date_1 == sanitised_date_2


class Time(object):
    hour: int
    minute: int
    second: int
    millis: int

    def __init__(self, hour: int, minute: int, second: int, millis: int):
        self.hour = hour
        self.minute = minute
        self.second = second
        self.millis = millis

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return "%d:%d:%d.%d" % (self.hour, self.minute, self.second, self.millis)

    @staticmethod
    def from_json(obj: Dict[str, Any]) -> "Time":
        return Time(
            0 if "hour" not in obj else obj["hour"],
            0 if "minute" not in obj else obj["minute"],
            0 if "second" not in obj else obj["second"],
            0 if "millis" not in obj else obj["millis"],
        )

    def to_json(self):
        return {
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
            "millis": self.millis,
        }

    @staticmethod
    def from_utc_timestamp(tmstamp: str) -> "Time":
        if tmstamp is None:
            return None

        tmstamp_split = tmstamp.split("T")

        if len(tmstamp_split) < 2:
            return None

        return Time.from_timepart(tmstamp_split[1])

    @staticmethod
    def from_timepart(tmstamp: str) -> "Time":
        if tmstamp is None:
            return None

        # strip timezone
        if tmstamp.endswith("Z"):
            tmstamp = tmstamp[:-1]
        elif tmstamp.endswith("SAST"):
            tmstamp = tmstamp[:-4]

        try:
            # split on millis
            tmstamp_split = tmstamp.split(".")
            # split non-millis section and cast each to int
            time_split = list(map(lambda item: int(item), tmstamp_split[0].split(":")))
            # construct base object
            timestamp_object = Time(0, 0, 0, 0)

            # set hours, mins, seconds
            timestamp_object.hour = time_split[0]

            if len(time_split) >= 2:
                timestamp_object.minute = time_split[1]

            if len(time_split) == 3:
                timestamp_object.second = time_split[2]

            # set milliseconds
            if len(tmstamp_split) == 2:
                timestamp_object.millis = int(tmstamp_split[1])

            return timestamp_object
        except:
            return None


class TimeWindow(object):
    start: Time
    end: Time

    def __init__(self, start: Time, end: Time):
        self.start = start
        self.end = end

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return "%s-%s" % (self.start, self.end)

    @staticmethod
    def from_json(obj: Dict[str, Any]) -> "TimeWindow":
        return TimeWindow(
            None if "start" not in obj else Time.from_json(obj["start"]),
            None if "end" not in obj else Time.from_json(obj["end"]),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "start": None if self.start is None else self.start.to_json(),
            "end": None if self.end is None else self.end.to_json(),
        }

    def is_time_in_window(self, time: Time) -> bool:
        _end = self.end
        _time = time

        # if the window crosses the 24-hour mark, for simplicity sake, we add 24 hours to the end
        # i.e. 22:00 till 02:00 becomes 22:00 till 26:00
        if self.end.hour < self.start.hour:
            _end = Time(
                self.end.hour + 24, self.end.minute, self.end.second, self.end.millis
            )

            # we need to apply the same check to the input time IFF the end time was adjusted
            # i.e. if given time is smaller than the window start time, e.g. 01:00, we need to extend it to 25:00
            if time.hour < self.start.hour:
                _time = Time(time.hour + 24, time.minute, time.second, time.millis)

        # check if the hour is within range
        if _time.hour < self.start.hour or _time.hour > _end.hour:
            return False

        # check if the input time is greater or equal to the start time
        if _time.hour == self.start.hour:
            if _time.minute < self.start.minute:
                return False

            if _time.minute == self.start.minute:
                if _time.second < self.start.second:
                    return False

                if _time.second == self.start.second:
                    return _time.millis < self.start.millis

        # check if the input time is lesser or equal to the end time
        if _time.hour == self.end.hour:
            if _time.minute > self.end.minute:
                return False

            if _time.minute == self.end.minute:
                if _time.second > self.end.second:
                    return False

                if _time.second == self.end.second:
                    return _time.millis > self.end.millis

        # we know that the time is inside the window since we checked the boundaries
        return True
