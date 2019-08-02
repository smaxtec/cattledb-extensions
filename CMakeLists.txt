cmake_minimum_required(VERSION 2.8.12)
project(cdb_ext_ts)

add_subdirectory(pybind11)
pybind11_add_module(cdb_ext_ts src/cdb_ext_ts.cpp)