#!/usr/bin/python
# coding: utf8

import unittest
import random
import pendulum
import logging
import json

from cdb_ext import FastTSList


class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_base(self):
        t1 = FastTSList("hellö", "world")

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
        t1 = FastTSList("abc", "def")
        dt = pendulum.now("Europe/Vienna").replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[1] == dt.offset
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.iso_at_index(0)[0] == iso_str

    def test_canada(self):
        t1 = FastTSList("abc", "def")
        dt = pendulum.datetime(2019, 2, 12, 8, 15, 32, tz='America/Toronto').replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == -5*3600
        assert t1.iso_at_index(0)[0] == "2019-02-12T08:15:32-05:00"

    def test_vienna(self):
        t1 = FastTSList("abc", "def")
        dt = pendulum.datetime(2008, 3, 3, 12, 0, 0, tz='Europe/Vienna')
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == 3600
        assert t1.iso_at_index(0)[0] == "2008-03-03T12:00:00+01:00"
