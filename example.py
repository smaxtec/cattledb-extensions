import cProfile
import contextlib
import os
import random
import json
import pendulum

from cdb_ext import FastTSList, PyTSList


@contextlib.contextmanager
def profile(*args, **kwargs):
    profile = cProfile.Profile(*args, **kwargs)

    profile.enable()
    yield
    profile.disable()

    profile.print_stats()


t1 = FastTSList("hellö", "world")
t2 = PyTSList("hellö", "world")

start = pendulum.now("Europe/Vienna").subtract(minutes=110000)

l1 = [(start.add(minutes=n).isoformat(), random.random()) for n in range(50000)]
l2 = [(start.add(minutes=n).isoformat(), random.random()) for n in reversed(range(50000, 100000))]
l3 = [(start.add(minutes=n).isoformat(), random.random()) for n in range(100000, 110000)]
random.shuffle(l3)
data = l1 + l3 + l2


with profile():
    for ts, val in data:
        t1.insert_iso(ts, val)
    t1_json = json.dumps(t1.serializable())


with profile():
    for ts, val in data:
        t2.insert_iso(ts, val)
    t2_json = json.dumps(t2.serializable())


j = json.loads(t1_json)


assert len(t1) == len(t2) == 110000

j1 = json.loads(t1_json)
j2 = json.loads(t1_json)
assert len(j1) == len(j2) == 110000

print(t1.at_index(0), "-", t1.at_index(len(t1)-1))
print(t2.at_index(0), "-", t2.at_index(len(t2)-1))
print(t1.iso_at_index(0), "-", t1.iso_at_index(len(t1)-1))
print(t2.iso_at_index(0), "-", t2.iso_at_index(len(t2)-1))

assert len(t1.to_list()) == len(t2.to_list())

print("finished")