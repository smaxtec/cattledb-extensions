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

        assert t1.iso_at_index(0)[0] == start.replace(microsecond=0).isoformat()
        assert t1.iso_at_index(len(t1)-1)[0] == end.replace(microsecond=0).isoformat()
