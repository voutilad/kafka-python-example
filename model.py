"""
Our data model.
"""
from time import time_ns
from uuid import uuid4

class SensorValue:
    """Our data class for demonstration purposes."""
    def __init__(self, value: int):
        self.ts = int(time_ns() / 1_000_000)
        self.uuid = str(uuid4())
        self.value = value
        self._idx = 0
        self._fields = ["ts", "uuid", "value"]

    @classmethod
    def to_dict(cls, value, ctx):
        return dict(timestamp=value.ts,
                    identifier=value.uuid,
                    value=value.value)

    @classmethod
    def from_dict(cls, d, ctx):
        """Sloppy implementation."""
        sv = SensorValue(d.get("value", -1))
        sv.uuid = d.get("identifier", "")
        sv.ts = d.get("timestamp", 0)
        return sv

    def __str__(self):
        return f"SensorValue({self.ts}, {self.uuid}, {self.value})"
