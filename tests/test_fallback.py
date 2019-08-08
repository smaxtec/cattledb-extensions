#!/usr/bin/python
# coding: utf8

import unittest
import random
import pendulum
import logging
import json

from cdb_ext import PyTSList


class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_base(self):
        t1 = PyTSList("hellö", "world")

        end = pendulum.now("Europe/Vienna")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n).isoformat(), random.random()) for n in range(0, 200)]
        random.shuffle(data)

        for ts, val in data:
            t1.insert_iso(ts, val)
        t1_json = json.dumps(t1.serializable())

        j1 = json.loads(t1_json)

        assert len(j1) == 200
        assert len(t1.to_list()) == 200

        assert t1.at_index(0)[1] == start.offset
        assert t1.at_index(0)[0] == start.int_timestamp

        assert t1.iso_at_index(0)[0] == start.replace(microsecond=0).isoformat()
        assert t1.iso_at_index(len(t1)-1)[0] == end.replace(microsecond=0).isoformat()

    def test_timezone(self):
        t1 = PyTSList("abc", "def")
        dt = pendulum.now("Europe/Vienna").replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[1] == dt.offset
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.iso_at_index(0)[0] == iso_str

    def test_canada(self):
        t1 = PyTSList("abc", "def")
        dt = pendulum.datetime(2019, 2, 12, 8, 15, 32, tz='America/Toronto').replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == -5*3600
        assert t1.iso_at_index(0)[0] == "2019-02-12T08:15:32-05:00"

    def test_vienna(self):
        t1 = PyTSList("abc", "def")
        dt = pendulum.datetime(2008, 3, 3, 12, 0, 0, tz='Europe/Vienna')
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == 3600
        assert t1.iso_at_index(0)[0] == "2008-03-03T12:00:00+01:00"

    def test_trim(self):
        t1 = PyTSList("a", "b")

        end = pendulum.now("Europe/Vienna")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n), random.random()) for n in range(0, 200)]

        for dt, val in data:
            t1.insert_datetime(dt, val)

        assert len(t1) == 200
        t1.trim_index(100, 200)
        assert len(t1) == 100
        t1.trim_ts(start, end)
        assert len(t1) == 100
        t1.trim_ts(end.subtract(minutes=9), end)
        assert len(t1) == 10

    def test_index(self):
        t = PyTSList("a", "b")

        end = pendulum.now("America/Toronto")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n), random.random()) for n in range(0, 200)]
        for dt, val in data:
            t.insert_datetime(dt, val)

        print(t.at_index(199))
        assert t.index_of_ts(end) == 199
        assert t.index_of_ts(end.subtract(minutes=3)) == 196

        with self.assertRaises(KeyError):
            t.index_of_ts(end.subtract(seconds=1)) 

        with self.assertRaises(KeyError):
            del t[-1]
        with self.assertRaises(KeyError):
            t.index_of_ts(end.add(seconds=1))

        last_ts = t.nearest_index_of_ts(end.add(seconds=1))
        assert last_ts == 199

        prev_ts = t.nearest_index_of_ts(end.subtract(seconds=40))
        assert prev_ts == 198
