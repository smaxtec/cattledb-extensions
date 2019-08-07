# cdb_ext

[![Build Status](https://travis-ci.org/wuttem/cdb_ext.svg?branch=master)](https://travis-ci.org/wuttem/cdb_ext)

Extensions and Common Helpers für CattleDB

The `timeseries` classes provide a ordered list of timestamps with a timestamp offset and a float value.

*insert*
Insert is always sorted. O(1) for appending or prepending items. O(log n) for inserts.

*access*
By index access is O(1). The more common access by timestamp is O(log n).

## Usage

Initialize one of the main classes.

```python
from cdb_ext import TSList, FastTSList, PyTSList

# use fast c++ version
ts = FastTSList("key", "metric")

# use python version
ts = PyTSList("key", "metric")

# use best available
ts = TSList("key", "metric")
```

Add items using Python syntax. Timezone offsets can be given as tuple or using pendulum datetimes.

```python
import pendulum
import random
from cdb_ext import FastTSList

ts = FastTSList("my_device", "temperature")


start = pendulum.now("Europe/Vienna")
for i in range(24):
    ts[start.add(hours=i)] = random.random() * 30.0

# access by ts (other timezone)
print(ts[start])

# access by index
print(ts.at_index(2))

# trim to 3 items
ts.trim_index(2, 4)

# remove item
del ts[start.add(hours=3)]

# iterate raw
for t in ts:
    print(t)

# iterate iso datetime tuples
for t in ts.iter_iso():
    print(t)

# iterate pendulum datetime tuples
for t in ts.iter_datetime():
    print(t)
```

## Building
To build the extension cmake is needed.
```
python setup.py develop
pytest tests

# creating windows distribution
python setup.py sdist bdist_wheel
```
