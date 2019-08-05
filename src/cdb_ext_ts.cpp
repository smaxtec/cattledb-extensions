#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <ctime>

using namespace std;
namespace py = pybind11;


struct data_item {
  int64_t ts;
  int32_t ts_offset;
  double value;

  const std::array<char, sizeof("2019-08-01T09:41:01+00:00")> iso_format() const {
      char buf[sizeof("2019-08-01T09:41:01+00:00")];
      char add[sizeof("+00:00")];
      time_t timeGMT = (time_t) (ts + ts_offset);
      uint32_t hours = (uint32_t) (ts_offset / 3600);
      uint32_t minutes = (uint32_t) ((ts_offset / 60) % 60);
      std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S+00:00", std::gmtime(&timeGMT));
      std::sprintf(add, "+%02d:%02d", hours%100, minutes%100);

      std::array<char, sizeof("2019-08-01T09:41:01+00:00")> arr;
      memcpy(&arr[0], buf, sizeof("2019-08-01T09:41:01+00:00"));
      memcpy(&arr[sizeof("2019-08-01T09:41:01")-1], add, sizeof("+00:00"));

      return arr;
  }

  const std::array<char, sizeof(int64_t) + sizeof(int32_t) + sizeof(double)> to_bytes() const {
      union {
        int64_t int_var;
        char byte_array[sizeof(int64_t)];  // Size 8
      } uint64_u;

      union {
        int32_t int_var;
        char byte_array[sizeof(int32_t)];  // Size 4
      } int32_u;

      union {
        double double_var;
        char byte_array[sizeof(double)];  // size 8
      } double_u;

      uint64_u.int_var = ts;
      int32_u.int_var = ts_offset;
      double_u.double_var = value;

      std::array<char, sizeof(int64_t) + sizeof(int32_t) + sizeof(double)> arr;

      memcpy(&arr[0], uint64_u.byte_array, sizeof(int64_t));
      memcpy(&arr[sizeof(int64_t)], int32_u.byte_array, sizeof(int32_t));
      memcpy(&arr[sizeof(int64_t) + sizeof(int32_t)], double_u.byte_array, sizeof(double));

      return arr;
  }
};


namespace pybind11 { namespace detail {
    template <> struct type_caster<data_item> {
    public:
        /**
         * This macro establishes the name 'data_item' in
         * function signatures and declares a local variable
         * 'value' of type data_item
         */
        PYBIND11_TYPE_CASTER(data_item, _("data_item"));

        /**
         * Conversion part 1 (Python->C++): convert a PyObject into a data_item
         * instance or return false upon failure. The second argument
         * indicates whether implicit conversions should be applied.
         */
        bool load(handle src, bool) {
            /* Extract PyObject from handle */
            PyObject *source = src.ptr();
            if (!PyTuple_Check(source))
                return false;
            /* Now try to convert into data_item */
            value.ts = PyLong_AsLong(PyTuple_GetItem(source, 0));
            value.ts_offset = PyLong_AsLong(PyTuple_GetItem(source, 1));
            value.value = PyFloat_AsDouble(PyTuple_GetItem(source, 2));
            // Py_DECREF(source);
            /* Ensure return code was OK (to avoid out-of-range errors etc) */
            return !(value.ts == -1 && !PyErr_Occurred());
        }

        /**
         * Conversion part 2 (C++ -> Python): convert an data_item instance into
         * a Python object. The second and third arguments are used to
         * indicate the return value policy and parent object (for
         * ``return_value_policy::reference_internal``) and are generally
         * ignored by implicit casters.
         */
        static handle cast(data_item src, return_value_policy /* policy */, handle /* parent */) {
            return PyTuple_Pack(3, PyLong_FromLong(src.ts), PyLong_FromLong(src.ts_offset), PyFloat_FromDouble(src.value));
        }
    };
}}

typedef std::deque<data_item> data_deque;
typedef std::tuple<std::string, double> iso_item;

class timeseries {
    public:
        timeseries(const std::string &key, const std::string &metric) : 
            key(key), metric(metric), _min_ts(0), _max_ts(0) { }

        void setKey(const std::string &key_) { key = key_; }

        const std::string &get_key() const { return key; }

        const std::string my_repr() const { return "<timeseries '" + key + "." + metric + "'>"; }

        const size_t my_len() const { return _data.size(); }

        bool remove_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);

            if ((*idx).ts == ts) {
                _data.erase(idx);
                return true;
            }

            if (idx != _data.begin() || (*--idx).ts == ts) {
                _data.erase(idx);
                return true;
            }

            return false;
        }

        bool remove(const size_t &i) {
            _data.erase(_data.begin() + i);
            return true;
        }

        void trim_idx(const size_t &start_idx, const size_t &end_idx) {
            int erase_left = start_idx;
            int erase_right = _data.size() - end_idx;
            if (erase_left >= 1 && erase_left < _data.size())
                _data.erase(_data.begin(), _data.begin() + erase_left);
            if (erase_right >= 1 && erase_right <= _data.size())
                _data.erase(_data.begin() + _data.size() - (erase_right - 1), _data.end());
        }

        void trim_ts(const int64_t &start_ts, const int64_t &end_ts) {
            int idx1 = bisect_left(start_ts);
            int idx2 = bisect_left(end_ts);
            trim_idx(idx1, idx2);
        }

        const iso_item iso_at(const size_t &i) const {
            data_item d = at(i);
            auto arr = d.iso_format();
            std::string str(begin(arr), end(arr)-1);
            return std::make_tuple(str, d.value);
        }

        const py::bytes bytes_at(const size_t &i) const {
            data_item d = at(i);
            auto arr = d.to_bytes();
            std::string str(begin(arr), end(arr));
            return str;
        }

        const data_item &at(const size_t &i) const {
            return _data.at(i);
        }

        const data_item &at_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            if (idx == _data.begin()) {
                return *idx;
            }
            if (idx == _data.end()) {
                return *--idx;
            }
            int64_t t2 = (*idx).ts;
            int64_t t1 = (*--idx).ts;
            if (abs(ts - t1) <= abs(ts - t2))
                return *idx;
            return (*++idx);
        }

        const size_t index_of_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            if (idx == _data.begin()) {
                return idx - _data.begin();
            }
            if (idx == _data.end()) {
                return --idx - _data.begin();
            }
            int64_t t2 = (*idx).ts;
            int64_t t1 = (*--idx).ts;
            if (abs(ts - t1) <= abs(ts - t2))
                return idx - _data.begin();
            return ++idx - _data.begin();
        }

        static bool data_compare_left(const data_item& obj, int64_t ts) { return obj.ts < ts; }
        static bool data_compare_right(int64_t ts, const data_item& obj) { return ts < obj.ts; }

        const size_t bisect_left(const int64_t &ts) const {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            return idx - _data.begin();
        }

        const size_t bisect_right(const int64_t &ts) const {
            auto idx = upper_bound(_data.begin(), _data.end(), ts, data_compare_right);
            return idx - _data.begin();
        }

        bool insert_iso(const std::string &iso_ts, const double &value) {
            int y,M,d,h,m;
            float s;
            int tzh = 0, tzm = 0;
            int32_t ts_offset = 0;
            int parsed_cnt = sscanf(iso_ts.c_str(), "%d-%d-%dT%d:%d:%f%d:%dZ", &y, &M, &d, &h, &m, &s, &tzh, &tzm);

            if (parsed_cnt > 6) {
                if (tzh < 0) {
                    tzm = -tzm;
                }
                ts_offset = (tzm * 60) + (tzh * 3600);
            }

            tm time;
            time.tm_year = y - 1900; // Year since 1900
            time.tm_mon = M - 1;     // 0-11
            time.tm_mday = d;        // 1-31
            time.tm_hour = h;        // 0-23
            time.tm_min = m;         // 0-59
            time.tm_sec = (int)s;    // 0-61 (0-60 in C++11)

            int64_t ts = (int64_t) timegm(&time);
            ts = ts - ts_offset;
            return insert(ts, ts_offset, value);
        }

        bool insert(const int64_t &ts, const int32_t &ts_offset, const double &value) {
            data_item d = {ts, ts_offset, value};
            // Empty
            if (_data.size() == 0) {
                _data.push_back(d);
                _max_ts = ts;
                _min_ts = ts;
                return true;
            }

            // Insert Back
            if (ts > _max_ts) {
                _data.push_back(d);
                _max_ts = ts;
                return true;
            }
            
            // Insert Front
            if (ts < _min_ts) {
                _data.push_front(d);
                _min_ts = ts;
                return true;
            }

            // Insert Mid
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            data_item &item = _data.at(idx - _data.begin());
            // Replace
            if (item.ts == ts) {
                item.ts_offset = ts_offset;
                item.value = value;
                return false;
            }

            _data.insert(idx, d);
            return true;
        }

        const int64_t &get_min_ts() const { return _min_ts; }
        const int64_t &get_max_ts() const { return _max_ts; }

    public:
        std::string key;
        std::string metric;

    private:
        data_deque _data;
        int64_t _min_ts;
        int64_t _max_ts;
};


PYBIND11_MODULE(cdb_ext_ts, m) {
    m.doc() = "CattleDB TS C Extensions";
    py::class_<timeseries>(m, "timeseries")
        .def(py::init<const std::string &, const std::string &>())
        .def_readwrite("key", &timeseries::key)
        .def_readwrite("metric", &timeseries::metric)
        .def("insert", &timeseries::insert)
        .def("insert_iso", &timeseries::insert_iso)
        .def("at", &timeseries::at)
        .def("at_ts", &timeseries::at_ts)
        .def("index_of_ts", &timeseries::index_of_ts)
        .def("iso_at", &timeseries::iso_at)
        .def("bytes_at", &timeseries::bytes_at)
        .def("bisect_left", &timeseries::bisect_left)
        .def("bisect_right", &timeseries::bisect_right)
        .def("trim_idx", &timeseries::trim_idx)
        .def("trim_ts", &timeseries::trim_ts)
        .def("get_min_ts", &timeseries::get_min_ts)
        .def("get_max_ts", &timeseries::get_max_ts)
        .def("remove_ts", &timeseries::remove_ts)
        .def("remove", &timeseries::remove)
        .def("__len__", &timeseries::my_len)
        .def("__repr__", &timeseries::my_repr);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
