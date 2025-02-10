from typing import Any, Dict


class Time:
    hours: int
    minutes: int
    seconds: int

    def __init__(self, hours: int = 0, minutes: int = 0, seconds: int = 0) -> None:
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def to_string(self) -> str:
        return f"{self.hours:02}:{self.minutes:02}:{self.seconds:02}"

    @staticmethod
    def from_string(time_str: str) -> "Time":
        time_split = time_str.split(":")

        return Time(
            int(time_split[0]),
            int(time_split[1]),
            int(time_split[2]),
        )

    @staticmethod
    def from_json(obj: Dict[str, Any]) -> "Time":
        return Time(
            obj["hours"],
            obj["minutes"],
            obj["seconds"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "hours": self.hours,
            "minutes": self.minutes,
            "seconds": self.seconds,
        }


class Schedule:
    start_time: Time
    end_time: Time

    def __init__(self, start_time: Time = None, end_time: Time = None) -> None:
        self.start_time = Time() if start_time is None else start_time
        self.end_time = Time() if end_time is None else end_time

    def to_string(self) -> str:
        return f"{self.start_time.to_string()}_{self.end_time.to_string()}"

    @staticmethod
    def from_string(sch_str: str) -> "Schedule":
        sch_split = sch_str.split("_")

        return Schedule(
            Time.from_string(sch_split[0]),
            Time.from_string(sch_split[1]),
        )

    @staticmethod
    def from_json(obj: Dict[str, Any]) -> "Schedule":
        if obj is None:
            return None

        return Schedule(
            Time.from_json(obj["startTime"]),
            Time.from_json(obj["endTime"]),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "startTime": self.start_time.to_json(),
            "endTime": self.end_time.to_json(),
        }
