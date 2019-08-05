import pendulum
import datetime

from ._py_ts import py_timeseries

try:
    from cdb_ext_ts import timeseries
    c_container = timeseries
    c_ext = True
except ImportError:  # pragma: no cover
    c_container = None
    c_ext = False


def extract_ts(ts):
    if isinstance(ts, int):
        return ts, 0
    elif isinstance(ts, float):
        return int(ts), 0
    elif isinstance(ts, pendulum.DateTime):
        return ts.int_timestamp, ts.offset
    elif isinstance(ts, datetime.datetime):
        pd = pendulum.instance(ts)
        return ts.int_timestamp, ts.offset
    elif isinstance(ts, tuple):
        return int(ts[0]), int(ts[1])
    raise TypeError("invlid value ({}){} for ts".format(type(ts), ts))


class StreamList(list):
    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        return self.iterator()


class _TSList(object):
    __container__ = None

    def __init__(self, key, metric):
        self._data = self.__container__(key, metric)

    def insert(self, ts, ts_offset, value):
        self._data.insert(ts, ts_offset, value)

    def insert_iso(self, iso_ts, value):
        return self._data.insert_iso(iso_ts, value)

    def __getitem__(self, key):
        timestamp, offset = extract_ts(key)
        return self.at_ts(timestamp)

    def __len__(self):
        return len(self._data)

    def __setitem__(self, key, value):
        timestamp, offset = extract_ts(key)
        self._data.insert(timestamp, offset, value)

    def __delitem__(self, key):
        timestamp, offset = extract_ts(key)
        self.remove_ts(timestamp)

    def at_ts(self, i):
        return self._data.at_ts(i)

    def at_index(self, i):
        return self._data.at(i)

    def iso_at_index(self, i):
        return self._data.iso_at(i)

    def bytes_at_index(self, i):
        return self._data.bytes_at(i)

    def remove_ts(self, ts):
        timestamp, offset = extract_ts(ts)
        return self._data.remove_ts(timestamp)

    def remove_index(self, idx):
        return self._data.remove(idx)

    @property
    def key(self):
        return self._data.key

    @property
    def metric(self):
        return self._data.metric

    def index_of_ts(self, ts):
        return self._data.index_of_ts(ts)

    def __iter__(self):
        i = 0
        while i < len(self):
            yield self.at_index(i)
            i += 1

    def _iterate_serializables(self):
        i = 0
        while i < len(self):
            yield self.iso_at_index(i)
            i += 1

    def serializable(self):
        return StreamList(self._iterate_serializables)

    def to_list(self):
        return list([x for x in self])


class PyTSList(_TSList):
    __container__ = py_timeseries

class FastTSList(_TSList):
    __container__ = c_container


if c_ext:
    TSList = FastTSList
else:
    TSList = PyTSList
