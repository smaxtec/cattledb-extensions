# cdb_ext
Extensions and Common Helpers für CattleDB

The `timeseries` classes provide a ordered list of timestamps with a timestamp offset and a float value.

*insert*
Insert is always sorted. O(1) for appending or prepending items. O(log n) for inserts.

*access*
By index access is O(1). The more common access by timestamp is O(log n).

## Usage

```python
from cdb_ext import TSList, FastTSList, PyTSList

# use fast c++ version
ts = FastTSList("key", "metric")

# use python version
ts = PyTSList("key", "metric")

# use best available
ts = TSList("key", "metric")
```

## Building
To build the extension cmake is needed.
```
python setup.py develop
pytest tests

# creating windows distribution
python setup.py sdist bdist_wheel
```
