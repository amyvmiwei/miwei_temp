/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */
#include "Common/Compat.h"

#include "../ThriftBroker/SerializedCellsReader.h"
#include "../ThriftBroker/SerializedCellsWriter.h"

#include <boost/python.hpp>

using namespace Hypertable;
using namespace boost::python;

typedef bool (SerializedCellsWriter::*addfn)(const char *row, 
                const char *column_family, const char *column_qualifier, 
                int64_t timestamp, const char *value, int32_t value_length, 
                int cell_flag);
typedef const char *(SerializedCellsWriter::*getfn)();
typedef int32_t (SerializedCellsWriter::*getlenfn)();

static addfn afn = &Hypertable::SerializedCellsWriter::add;
static getlenfn lenfn = &Hypertable::SerializedCellsWriter::get_buffer_length;

static PyObject *convert(const SerializedCellsWriter &scw) {
  boost::python::object obj(handle<>(PyBuffer_FromMemory(
                      (void *)scw.get_buffer(), scw.get_buffer_length())));
  return boost::python::incref(obj.ptr());
}

BOOST_PYTHON_MODULE(libHyperPython)
{
  class_<SerializedCellsReader>("SerializedCellsReader", 
          init<const char *, uint32_t>())
    .def("has_next", &SerializedCellsReader::next)
    .def("row", &SerializedCellsReader::row,
          return_value_policy<return_by_value>())
    .def("column_family", &SerializedCellsReader::column_family,
          return_value_policy<return_by_value>())
    .def("column_qualifier", &SerializedCellsReader::column_qualifier,
          return_value_policy<return_by_value>())
    .def("value", &SerializedCellsReader::value_str,
          return_value_policy<return_by_value>())
    .def("value_len", &SerializedCellsReader::value_len)
    .def("timestamp", &SerializedCellsReader::timestamp)
    .def("cell_flag", &SerializedCellsReader::cell_flag)
    .def("flush", &SerializedCellsReader::flush)
    .def("eos", &SerializedCellsReader::eos)
  ;

  class_<SerializedCellsWriter, boost::noncopyable>("SerializedCellsWriter",
          init<int32_t, bool>())
    .def("add", afn)
    .def("finalize", &SerializedCellsWriter::finalize)
    .def("empty", &SerializedCellsWriter::empty)
    .def("clear", &SerializedCellsWriter::clear)
    .def("__len__", lenfn)
    .def("get", &convert)
  ;
}
