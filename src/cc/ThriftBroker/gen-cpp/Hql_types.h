/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef Hql_TYPES_H
#define Hql_TYPES_H

#include <Thrift.h>
#include <reflection_limited_types.h>
#include <protocol/TProtocol.h>
#include <transport/TTransport.h>

#include "Client_types.h"


namespace Hypertable { namespace ThriftGen {

class HqlResult {
 public:

  static const char* ascii_fingerprint; // = "18145C760E907938802BB3CC3490043E";
  static const uint8_t binary_fingerprint[16]; // = {0x18,0x14,0x5C,0x76,0x0E,0x90,0x79,0x38,0x80,0x2B,0xB3,0xCC,0x34,0x90,0x04,0x3E};

  HqlResult() : scanner(0), mutator(0) {
  }

  virtual ~HqlResult() throw() {}

  std::vector<std::string>  results;
  std::vector<Hypertable::ThriftGen::Cell>  cells;
  int64_t scanner;
  int64_t mutator;

  struct __isset {
    __isset() : results(false), cells(false), scanner(false), mutator(false) {}
    bool results;
    bool cells;
    bool scanner;
    bool mutator;
  } __isset;

  bool operator == (const HqlResult & rhs) const
  {
    if (__isset.results != rhs.__isset.results)
      return false;
    else if (__isset.results && !(results == rhs.results))
      return false;
    if (__isset.cells != rhs.__isset.cells)
      return false;
    else if (__isset.cells && !(cells == rhs.cells))
      return false;
    if (__isset.scanner != rhs.__isset.scanner)
      return false;
    else if (__isset.scanner && !(scanner == rhs.scanner))
      return false;
    if (__isset.mutator != rhs.__isset.mutator)
      return false;
    else if (__isset.mutator && !(mutator == rhs.mutator))
      return false;
    return true;
  }
  bool operator != (const HqlResult &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const HqlResult & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

}} // namespace

#endif
