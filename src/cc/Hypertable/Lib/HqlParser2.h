/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_HQLPARSER2_H
#define HYPERTABLE_HQLPARSER2_H

//#define BOOST_SPIRIT_DEBUG  ///$$$ DEFINE THIS WHEN DEBUGGING $$$///

#ifdef BOOST_SPIRIT_DEBUG
#define BOOST_SPIRIT_DEBUG_OUT std::cerr
#define HQL_DEBUG(_expr_) std::cerr << __func__ <<": "<< _expr_ << std::endl
#define HQL_DEBUG_VAL(str, val) HQL_DEBUG(str <<" val="<< val)
#else
#define HQL_DEBUG(_expr_)
#define HQL_DEBUG_VAL(str, val)
#endif

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/qi_no_case.hpp>
#include <boost/spirit/include/phoenix_bind.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_fusion.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/spirit/include/phoenix_object.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>

#include <iostream>
#include <string>

#include <sys/types.h>

#include "KeySpec.h"

namespace Hql {

  const char *commands[] = {
    "???",
    "HELP",
    "CREATE_TABLE",
    "DESCRIBE_TABLE",
    "SHOW_CREATE_TABLE",
    "SELECT",
    "LOAD_DATA",
    "INSERT",
    "DELETE",
    "GET_LISTING",
    "DROP_TABLE",
    "ALTER_TABLE",
    "CREATE_SCANNER",
    "DESTROY_SCANNER",
    "FETCH_SCANBLOCK",
    "LOAD_RANGE",
    "SHUTDOWN",
    "SHUTDOWN_MASTER",
    "UPDATE",
    "REPLAY_BEGIN",
    "REPLAY_LOAD_RANGE",
    "REPLAY_LOG",
    "REPLAY_COMMIT",
    "DROP_RANGE",
    "DUMP",
    "CLOSE",
    "DUMP_TABLE",
    "EXISTS_TABLE",
    "USE_NAMESPACE",
    "CREATE_NAMESPACE",
    "DROP_NAMESPACE",
    "RENAME_TABLE",
    "WAIT_FOR_MAINTENANCE",
    "BALANCE",
    "HEAPCHECK",
    "COMPACT",
    "METADATA_SYNC",
    "STOP",
    (const char *)0
  };

  namespace fusion = boost::fusion;
  namespace phoenix = boost::phoenix;
  namespace qi = boost::spirit::qi;
  namespace ascii = boost::spirit::ascii;

  class split_date {
  public:
    split_date() : year(0), month(0), day(0), hour(0), minute(0), sec(0), nsec(0) { }
    void to_string() { std::cout << year << "-" << month << "-" << day << " " << hour << ":" << minute << ":" << sec << ":" << nsec << std::endl; }
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int sec;
    int nsec;
  };
  std::ostream &operator<<(std::ostream &os, const split_date &sd) {
    os << sd.year << "-" << sd.month << "-" << sd.day << " " << sd.hour << ":" << sd.minute << ":" << sd.sec << ":" << sd.nsec;
    return os;
  }

  typedef boost::variant< ::int64_t, split_date> date_variant;

  class date_variant_visitor  : public boost::static_visitor<> {
  public:
    date_variant_visitor(std::ostream &os) : out(os) { }
    void operator()(const split_date &sd) const { out << sd; }
    void operator()(const ::int64_t &i) const { out << i; }
    std::ostream &out;
  };
  class date_is_null : public boost::static_visitor<bool> {
  public:
    bool operator()(const split_date &sd) const { return false; }
    bool operator()(const ::int64_t &i) const { return i == Hypertable::TIMESTAMP_NULL; }
  };
  std::ostream &operator<<(std::ostream &os, const date_variant &dv) {
    boost::apply_visitor( date_variant_visitor(os), dv );
    return os;
  }

  class time_range {
  public:
    time_range() : start(Hypertable::TIMESTAMP_NULL), end(Hypertable::TIMESTAMP_NULL), start_inclusive(true), end_inclusive(true) {}
    date_variant start;
    date_variant end;
    bool start_inclusive;
    bool end_inclusive;
  };

  std::ostream &operator<<(std::ostream &os, const time_range &tr) {
    if (boost::apply_visitor(date_is_null(), tr.end))
      os << tr.start;
    else {
      os << ((tr.start_inclusive) ? "[" : "(");
      os << tr.start << "," << tr.end;
      os << ((tr.end_inclusive) ? "]" : ")");
    }
    return os;
  }

  class column_spec {
  public:
    column_spec() : regex(false) { }
    std::string name;
    std::string qualifier;
    bool regex;
  };

  std::ostream &operator<<(std::ostream &os, const column_spec &cspec) {
    os << cspec.name;
    if (!cspec.qualifier.empty()) {
      if (cspec.regex)
        os << ":/" << cspec.qualifier << "/";
      else
        os << ":" << cspec.qualifier;
    }
    return os;
  }

  class Value {
  public:
    Value() : type(0), count(0), revision(Hypertable::TIMESTAMP_AUTO) { }
    int type;
    std::string row;
    column_spec column;
    std::string value;
    time_range timestamp;
    unsigned int count;
    date_variant revision;
  };

  std::ostream &operator<<(std::ostream &os, const Value &v) {
    os << "(";
    if (v.type == 0)
      os << "INS,";
    else if (v.type == 1)
      os << "DEL,";
    else if (v.type == 2)
      os << "DEL1,";
    else
      os << "UNKNOWN,";
    os << v.row << ",";
    if (!v.column.name.empty()) {
      os << v.column << ",";
      if (!v.value.empty())
        os << v.value << ",";
    }
    os << v.timestamp << "," << v.count << "," << v.revision << ")";
    return os;
  }

  class ParserState {
  public:
    ParserState() : command(0) { }
    void set_command(int c) { command=c; }
    int command;
    std::string table_name;
    std::vector<Value> values;
    std::vector<column_spec> columns;
  };

  std::ostream &operator<<(std::ostream &os, const ParserState &state) {
    bool first=true;
    if (state.command == 5) {
      os << "SELECT ";
      foreach_ht (const column_spec &cspec, state.columns) {
        if (!first)
          os << ",";
        else
          first = false;
        os << cspec;
      }
      os << " FROM " << state.table_name;
    }
    else if (state.command == 7) {
      os << "INSERT INTO " << state.table_name << " VALUES ";
      foreach_ht (const Value &v, state.values)
        os << v;
    }
    return os;
  }


}


BOOST_FUSION_ADAPT_STRUCT(
                          Hql::time_range,
                          (Hql::date_variant, start)
                          (Hql::date_variant, end)
                          (bool, start_inclusive)
                          (bool, end_inclusive)
                          )

BOOST_FUSION_ADAPT_STRUCT(
                          Hql::split_date,
                          (int, year)
                          (int, month)
                          (int, day)
                          (int, hour)
                          (int, minute)
                          (int, sec)
                          (int, nsec)
                          )

BOOST_FUSION_ADAPT_STRUCT(
                          Hql::column_spec,
                          (std::string, name)
                          (std::string, qualifier)
                          (bool, regex)
                          )


BOOST_FUSION_ADAPT_STRUCT(
                          Hql::Value,
                          (int, type)
                          (std::string, row)
                          (Hql::column_spec, column)
                          (std::string, value)
                          (Hql::time_range, timestamp)
                          (unsigned int, count)
                          (Hql::date_variant, revision)
                          )

namespace Hql {

  using qi::symbols;

  template <typename Iterator>
  struct hql_parser : qi::grammar<Iterator, ParserState(), qi::locals<std::string>, ascii::space_type>
  {

    symbols<char, ::int64_t> date_symbols;
    symbols<char, int> command_symbols;
    
    hql_parser() : hql_parser::base_type(statement, "statement")
    {
      using qi::alpha;
      using qi::alnum;
      using qi::int_;
      using qi::uint_;
      using qi::lit;
      using qi::double_;
      using qi::long_long;
      using qi::lexeme;
      using qi::no_case;
      using qi::on_error;
      using qi::eps;
      using qi::fail;
      using ascii::char_;
      using ascii::string;
      using namespace qi::labels;

      using phoenix::construct;
      using phoenix::val;
      using phoenix::at_c;
      using phoenix::bind;
      using phoenix::push_back;
      using phoenix::ref;

      date_symbols.add
        ("auto", Hypertable::TIMESTAMP_AUTO)
        ("null", Hypertable::TIMESTAMP_NULL)
        ("min", Hypertable::TIMESTAMP_MIN)
        ("max", Hypertable::TIMESTAMP_MAX);

      for (int i=1; commands[i]; i++)
        command_symbols.add(commands[i], i);

      statement %=
        statement_select
        | statement_insert
        ;

      statement_select =
        no_case[lit("select")] [bind(&ParserState::command, _val) = command_symbols.at("SELECT")]
        >> !(no_case[lit("cells")])
        >> ('*' | (column_match_specification [push_back(bind(&ParserState::columns, _val), _1)]
                   >> *(',' >> column_match_specification [push_back(bind(&ParserState::columns, _val), _1)] )))
        >> no_case[lit("from")] >> user_identifier [bind(&ParserState::table_name, _val) = _1]
        ;

      statement_insert =
        no_case[lit("insert")] [bind(&ParserState::command, _val) = command_symbols.at("INSERT")]
        > no_case[lit("into")]
        > user_identifier [bind(&ParserState::table_name, _val) = _1]
        > no_case[lit("values")]
        > +( value [push_back(bind(&ParserState::values, _val), _1)] )
        ;

      user_identifier =
        identifier
        | quoted_string
        ;

      identifier %= lexeme[alpha >> *(alnum | char_('_'))];

      quoted_string =
        double_quoted_string
        | single_quoted_string
        ;

      double_quoted_string %= lexeme['"' >> +('\\' >> char_("\"") | ~char_('"')) >> '"'];

      single_quoted_string %= lexeme['\'' >> +('\\' >> char_("'") | ~char_('\'')) >> '\''];

      value =
        ( insert [_val = _1]
          | del [_val = _1]
          | delete_cell_version [_val = _1]
          )
        ;

      insert = '(' >>
        -(no_case[lit("ins")] >> ',')
        >> quoted_string [at_c<1>(_val) = _1] >> ','
        >> column_specification [at_c<2>(_val) = _1] >> ','
        >> quoted_string [at_c<3>(_val) = _1]
        >> -( ',' >> date_range [at_c<4>(_val) = _1]
             >> -( ',' >> uint_ [at_c<5>(_val) = _1]
                  >> -( ',' >> date [at_c<6>(_val) = _1])))
        >> char_(')')
        ;

      del = '(' >>
        no_case[lit("del")] [at_c<0>(_val) = 1] >> ','
        >> quoted_string [at_c<1>(_val) = _1]
        >> -( ',' >> column_specification [at_c<2>(_val) = _1]
              >> -( ',' >> date_range [at_c<4>(_val) = _1]
                    >> -( ',' >> uint_ [at_c<5>(_val) = _1]
                          >> -( ',' >> date [at_c<6>(_val) = _1]))))
        >> ')'
        ;

      delete_cell_version = '(' >>
        no_case[lit("del1")] [at_c<0>(_val) = 2] 
        >> ',' >> quoted_string [at_c<1>(_val) = _1] 
        >> ',' >> column_specification [at_c<2>(_val) = _1]
        >> ',' >> date_range [at_c<4>(_val) = _1]
        >> -( ',' >> date [at_c<6>(_val) = _1])
        >> ')'
        ;

      column_specification =
        user_identifier [at_c<0>(_val) = _1] 
        >> -( ':' >> user_identifier [at_c<1>(_val) = _1] )
        ;

      column_match_specification = 
        (user_identifier [at_c<0>(_val) = _1] 
         >> ':'
         >> regex [at_c<1>(_val) = _1]
         >> eps [at_c<2>(_val) = true])
        | column_specification [_val = _1]
        ;

      regex %= lexeme['/' >> +( ~char_('/') ) >> '/'];

      date_range = 
        date [at_c<0>(_val) = _1]
        | (( char_('[') [at_c<2>(_val) = true] | char_('(') [at_c<2>(_val) = false] )
           > date [at_c<0>(_val) = _1] > ',' > date [at_c<1>(_val) = _1] 
           > ( char_(']') [at_c<3>(_val) = true] | char_(')') [at_c<3>(_val) = false] ))
        ;

      date = 
        ( date_formatted [_val = _1]
          | time_since_epoch [_val = _1]
          | date_symbols  [_val = _1])
        ;

      date_formatted =
        int_ [at_c<0>(_val) = _1] >> !(lit('s') | lit("ns"))
        >> -('-' >> int_ [at_c<1>(_val) = _1]
             >> -( '-' >> int_ [at_c<2>(_val) = _1]
                   >> -( int_ [at_c<3>(_val) = _1]
                         >> -( ':' > int_ [at_c<4>(_val) = _1]
                               >> -( ':' > int_ [at_c<5>(_val) = _1]
                                     >> -(':' >> int_ [at_c<6>(_val) = _1]))))))
        ;

      time_since_epoch =
        ( long_long[_val = _1] >> "ns" )
        | ( long_long [_val = _1*1000000000LL] >> 's' )
        ;


      statement.name("statement");
      statement_insert.name("statement_select");
      statement_insert.name("statement_insert");
      identifier.name("identifier");
      user_identifier.name("user_identifier");
      quoted_string.name("quoted_string");
      double_quoted_string.name("double_quoted_string");
      single_quoted_string.name("single_quoted_string");
      value.name("value");
      del.name("del");
      delete_cell_version.name("delete_cell_version");
      column_specification.name("column_specification");
      column_match_specification.name("column_match_specification");
      regex.name("regex");
      insert.name("insert");
      date_range.name("date_range");
      date.name("date");
      date_formatted.name("date_formatted");
      time_since_epoch.name("time_since_epoch");

      on_error<fail>
        (
         statement
         , std::cout
         << val("Error: Expecting ")
         << _4                               // what failed?
         << val(" here: \"")
         << construct<std::string>(_3, _2)   // iterators to error-pos, end
         << val("\"")
         << std::endl
         );

#if 0
      debug(statement);
      debug(statement_select);
      debug(statement_insert);
      debug(identifier);
      debug(user_identifier);
      debug(quoted_string);
      debug(double_quoted_string);
      debug(single_quoted_string);
      debug(value);
      debug(insert);
      debug(del);
      debug(delete_cell_version);
      debug(column_specification);
      debug(column_match_specification);
      debug(regex);
      debug(date_range);
      debug(date);
      debug(date_formatted);
      debug(time_since_epoch);
#endif
    }

    qi::rule<Iterator, ParserState(), qi::locals<std::string>, ascii::space_type> statement;
    qi::rule<Iterator, ParserState(), ascii::space_type> statement_select;
    qi::rule<Iterator, ParserState(), ascii::space_type> statement_insert;
    qi::rule<Iterator, std::string(), ascii::space_type> identifier;
    qi::rule<Iterator, std::string(), ascii::space_type> user_identifier;
    qi::rule<Iterator, std::string(), ascii::space_type> quoted_string;
    qi::rule<Iterator, std::string(), ascii::space_type> double_quoted_string;
    qi::rule<Iterator, std::string(), ascii::space_type> single_quoted_string;
    qi::rule<Iterator, Value(), ascii::space_type> value;
    qi::rule<Iterator, Value(), ascii::space_type> insert;
    qi::rule<Iterator, Value(), ascii::space_type> del;
    qi::rule<Iterator, Value(), ascii::space_type> delete_cell_version;
    qi::rule<Iterator, column_spec(), ascii::space_type> column_specification;
    qi::rule<Iterator, column_spec(), ascii::space_type> column_match_specification;
    qi::rule<Iterator, std::string(), ascii::space_type> regex;
    qi::rule<Iterator, time_range(), ascii::space_type> date_range;
    qi::rule<Iterator, date_variant(), ascii::space_type> date;
    qi::rule<Iterator, split_date(), ascii::space_type> date_formatted;
    qi::rule<Iterator, ::int64_t(), ascii::space_type> time_since_epoch;
  };
}


#endif // HYPERTABLE_HQLPARSER2_H
