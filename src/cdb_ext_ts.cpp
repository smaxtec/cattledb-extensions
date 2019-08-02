#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <ctime>

using namespace std;
namespace py = pybind11;


struct data_item {
  int ts;
  int ts_offset;
  double value;

  const std::string iso_format() const {
      char buf[sizeof("2019-08-01T09:41:01")];
      char h[sizeof("00")];
      char m[sizeof("00")];
      time_t timeGMT = (time_t)ts;
      uint hours = (uint) (ts_offset / 3600);
      uint minutes = (uint) ((ts_offset / 60) % 60);
      std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::gmtime(&timeGMT));
      std::sprintf(h, "%02d", hours%100);
      std::sprintf(m, "%02d", minutes%100);
      if (ts_offset < 0)
          return (std::string) buf + "-" + h + ":" + m;
      return (std::string) buf + "+" + h + ":" + m;
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
            key(key), metric(metric), _min_ts(-1), _max_ts(-1) { }

        void setKey(const std::string &key_) { key = key_; }

        const std::string &get_key() const { return key; }

        const std::string my_repr() const { return "<timeseries '" + key + "." + metric + "'>"; }

        const int my_len() const { return _data.size(); }

        const data_deque &to_list() const { 
            return _data;
        }

        void trim_idx(const unsigned int &start_idx, const unsigned int &end_idx) {
            unsigned int erase_left = start_idx;
            unsigned int erase_right = _data.size() - end_idx;
            if (erase_left >= 1 && erase_left < _data.size())
                _data.erase(_data.begin(), _data.begin() + erase_left);
            if (erase_right >= 1 && erase_right <= _data.size())
                _data.erase(_data.begin() + _data.size() - (erase_right - 1), _data.end());
        }

        void trim_ts(const unsigned int &start_ts, const unsigned int &end_ts) {
            int idx1 = bisect_left(start_ts);
            int idx2 = bisect_left(end_ts);
            trim_idx(idx1, idx2);
        }

        const iso_item at_iso(const int &i) const {
            data_item d = at(i);
            return std::make_tuple(d.iso_format(), d.value);
        }

        const data_item &at(const int &i) const {
            if (i < 0) {
                return _data.at(_data.size() + i);
            }
            return _data.at(i);
        }

        static bool data_compare_left(const data_item& obj, int ts) { return obj.ts < ts; }
        static bool data_compare_right(int ts, const data_item& obj) { return ts < obj.ts; }

        const int bisect_left(const int &ts) const {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            return idx - _data.begin();
        }

        const int bisect_right(const int &ts) const {
            auto idx = upper_bound(_data.begin(), _data.end(), ts, data_compare_right);
            return idx - _data.begin();
        }

        void insert(const int &ts, const int &ts_offset, const double &value) {
            // Insert Back
            data_item d = {ts, ts_offset, value};
            if (ts > _max_ts) {
                _data.push_back(d);
                _max_ts = ts;
                // was empty
                if (_data.size() == 1) {
                    _min_ts = ts;
                }
            // Insert Front
            } else if (ts < _min_ts) {
                _data.push_front(d);
                _min_ts = ts;
            // Insert Mid
            } else {
                auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
                data_item &item = _data.at(idx - _data.begin());
                if (item.ts == ts) {
                    item.ts_offset = ts_offset;
                    item.value = value;
                } else {
                    _data.insert(idx, d);
                }
            }
        }

        const int &get_min_ts() const { return _min_ts; }
        const int &get_max_ts() const { return _max_ts; }

    public:
        std::string key;
        std::string metric;

    private:
        data_deque _data;
        int _min_ts;
        int _max_ts;
};


PYBIND11_MODULE(cdb_ext_ts, m) {
    m.doc() = "CattleDB TS C Extensions";
    py::class_<timeseries>(m, "timeseries")
        .def(py::init<const std::string &, const std::string &>())
        .def_readwrite("key", &timeseries::key)
        .def_readwrite("metric", &timeseries::metric)
        .def("to_list", &timeseries::to_list)
        .def("insert", &timeseries::insert)
        .def("at", &timeseries::at)
        .def("at_iso", &timeseries::at_iso)
        .def("bisect_left", &timeseries::bisect_left)
        .def("bisect_right", &timeseries::bisect_right)
        .def("trim_idx", &timeseries::trim_idx)
        .def("trim_ts", &timeseries::trim_ts)
        .def("get_min_ts", &timeseries::get_min_ts)
        .def("get_max_ts", &timeseries::get_max_ts)
        .def("__len__", &timeseries::my_len)
        .def("__repr__", &timeseries::my_repr);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
