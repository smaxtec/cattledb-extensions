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
        return pd.int_timestamp, pd.offset
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
        return self._data.insert(ts, ts_offset, value)

    def insert_datetime(self, dt, value):
        timestamp, offset = extract_ts(dt)
        return self._data.insert(timestamp, offset, value)

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

    def datetime_at_index(self, i):
        item = self.at_index(i)
        return (pendulum.from_timestamp(item[0], item[1]/3600.0), item[2])

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

    def index_of_ts(self, dt):
        ts, _ = extract_ts(dt)
        return self._data.index_of_ts(ts)

    def __iter__(self):
        return self._iterate_raw()

    def _iterate_raw(self):
        i = 0
        while i < len(self):
            yield self.at_index(i)
            i += 1

    def _iterate_serializable(self):
        i = 0
        while i < len(self):
            yield self.iso_at_index(i)
            i += 1

    def _iterate_datetime(self):
        i = 0
        while i < len(self):
            yield self.datetime_at_index(i)
            i += 1

    def serializable(self):
        return StreamList(self._iterate_serializable)

    def iter_iso(self):
        return StreamList(self._iterate_serializable)

    def iter_datetime(self):
        return StreamList(self._iterate_datetime)

    def iter_raw(self):
        return StreamList(self._iterate_raw)

    def to_list(self):
        return list([x for x in self])

    def trim_index(self, start_idx, end_idx):
        self._data.trim_idx(start_idx, end_idx)
        return self

    def trim_ts(self, start_dt, end_dt):
        start_ts, _ = extract_ts(start_dt)
        end_ts, _ = extract_ts(end_dt)
        self._data.trim_ts(start_ts, end_ts)
        return self

    def nearest_index_of_ts(self, dt):
        ts, _ = extract_ts(dt)
        return self._data.nearest_index_of_ts(ts)

    def to_iso_json(self):
        out = []
        for ts, value in self._iterate_serializable():
            out.append('["{}",{}]'.format(ts, value))
        return "[" + ",\n".join(out) + "]"


class PyTSList(_TSList):
    __container__ = py_timeseries

class FastTSList(_TSList):
    __container__ = c_container


if c_ext:
    TSList = FastTSList
else:
    TSList = PyTSList
