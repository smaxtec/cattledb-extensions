import bisect
import pendulum
import six


class KeyWrapper:
    key_getter = lambda c: c[0]

    def __init__(self, timeseries):
        self.timeseries = timeseries

    def __getitem__(self, i):
        return self.timeseries._data[i][0]

    def __len__(self):
        return len(self.timeseries)


class py_timeseries(object):
    def __init__(self, key, metric):
        self._data = list()
        self.key = key
        self.metric = metric

    def insert(self, ts, ts_offset, value):
        idx = self.bisect_left(ts)
        if idx < len(self) and self._data[idx][0] == ts:
            self._data[idx] = (ts, ts_offset, value)
            return False
        self._data.insert(idx, (ts, ts_offset, value))
        return True

    def insert_iso(self, iso_ts, value):
        dt = pendulum.parse(iso_ts)
        return self.insert(dt.int_timestamp, dt.offset, value)

    def bisect_left(self, ts):
        return bisect.bisect_left(KeyWrapper(self), ts)

    def bisect_right(self, ts):
        return bisect.bisect_right(KeyWrapper(self), ts)

    def at(self, key):
        return self._data[key]

    def at_ts(self, ts):
        idx = self.bisect_left(ts)
        item = self._data[idx]
        if item[0] == ts:
            return self._data[idx]
        raise KeyError("timestamp: {}".format(ts))

    def nearest_index_of_ts(self, ts):
        idx = self.bisect_left(ts)

        if idx == 0:
            return idx
        if idx == len(self):
            return idx-1 

        t2 = self._data[idx][0]
        t1 = self._data[idx-1][0]

        if abs(ts - t1) <= abs(ts - t2):
            return idx-1
        return idx

    def index_of_ts(self, ts):
        idx = self.bisect_left(ts)
        if idx == len(self):
            raise KeyError("timestamp: {}".format(ts))
        item = self._data[idx]
        if item[0] == ts:
            return idx
        raise KeyError("timestamp: {}".format(ts))

    def iso_at(self, key):
        t = self.at(key)
        dt = pendulum.from_timestamp(t[0], t[1]/3600.0)
        return (dt.isoformat(), t[2])

    def bytes_at(self, key):
        raise NotImplementedError()

    def __len__(self):
        return len(self._data)

    def trim_idx(self, start_idx, end_idx):
        assert 0 <= start_idx < len(self)
        assert 0 <= end_idx
        self._data = self._data[start_idx: end_idx+1]

    def trim_ts(self, start_ts, end_ts):
        idx1 = self.bisect_left(start_ts)
        idx2 = self.bisect_right(end_ts)
        if idx2 > 1:
            self.trim_idx(idx1, idx2-1)
        else:
            if six.PY2:
                del self._data[:]
            else:
                self._data.clear()

    def get_min_ts(self):
        return self._data[0][0]

    def get_max_ts(self):
        return self._data[-1][0]

    def remove_ts(self, ts):
        idx = self.bisect_left(ts)
        item = self._data[idx]
        if item[0] == ts:
            del self._data[idx]
            return
        raise KeyError("timestamp: {}".format(ts))

    def remove(self, key):
        del self._data[key]

    def __repr__(self):
        return repr(self._data)
