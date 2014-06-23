/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Definitions for XmlParser.
/// This file contains type definitions for XmlParser, an abstract base class
/// for XML document parsers.

#ifndef Common_XmlParser_h
#define Common_XmlParser_h

extern "C" {
#include <expat.h>
#include <strings.h>
}

#include <cstdint>
#include <initializer_list>
#include <stack>
#include <string>
#include <vector>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Base class for XML document parsers.
  /// This class is a base class from which can be derived simple parsers for
  /// XML documents.  It contains virtual methods to handle start and end
  /// element processing and also supports sub-parsing where the raw input text
  /// of certain elements is collected and passed into the virtual method
  /// sub_parse() for external parsing.  The following pseudo-code example
  /// illustrates how to use this class to create a simple XML parser.
  ///
  /// @code
  /// class XmlParserExample : public XmlParser {
  ///
  /// public:
  ///
  ///   XmlParserExample(MyClass *obj, const char *s, int len) :
  ///     XmlParser(s,len,{"SubParseElement"}), m_object(obj) {}
  ///
  ///    void start_element(const XML_Char *name, const XML_Char **atts) override {
  ///      if (m_element_stack.empty()) { // implies toplevel
  ///        for (int i=0; atts[i] != 0; i+=2) {
  ///          if (!strcasecmp(atts[i], "version"))
  ///            m_object->set_version(content_to_i32(atts[i], atts[i+1]));
  ///          else
  ///            /* throw exception ... */;
  ///        }
  ///     }
  ///     else if (strcasecmp(name, "Generation"))
  ///       /* throw exception ... */;
  ///    }
  ///
  ///    void end_element(const XML_Char *name, const string &content) override {
  ///      if (!strcasecmp(name, "Generation"))
  ///        m_object->set_generation(content_to_i64(name, content));
  ///      else if (!m_element_stack.empty())
  ///       /* throw exception ... */;
  ///    }
  ///
  ///    void sub_parse(const XML_Char *name, const char *s, int len) override {
  ///      if (!strcasecmp(name, "SubParseElement")) {
  ///        OtherClass *foo = external_parse_foo(s, len);
  ///        m_object->set_foo(foo);
  ///      }
  ///   }
  ///
  ///  private:
  ///
  ///    MyClass *m_object;
  /// };
  /// @endcode
  class XmlParser {
  public:

    /// Constructor.
    /// Initializes member variables #m_base and #m_length to <code>base</code>
    /// and <code>len</code>, respectively.  Initializes eXpat parser (#m_parser), setting
    /// the element handlers to start_element_handler() and
    /// end_element_handler(), the character data handler to
    /// character_data_handler(), and the user data to <b>this</b>.
    /// @param base Pointer to beginning of XML document
    /// @param len Length of XML document
    XmlParser(const char *base, int len);

    /// Constructor with sub-parser list.
    /// Delegates construction to XmlParser() and
    /// initializes #m_sub_parsers to <code>sub_parsers</code>.
    /// @param base Pointer to beginning of XML document
    /// @param len Length of XML document
    /// @param sub_parsers List of element names for which sub parsers exist
    XmlParser(const char *base, int len,
              const std::initializer_list<std::string> &sub_parsers);

    /// Destructor.
    /// Frees #m_parser.
    virtual ~XmlParser();

    void parse();

    /// Start element callback member function.
    /// This virtual member function can be defined in concrete parser classes
    /// to handle open element parsing.
    /// @param name Name of element
    /// @param atts Attribute list of even length with each attribute name
    /// immediately followed by its corresponding value
    virtual void start_element(const XML_Char *name, const XML_Char **atts) {};

    /// End element callback member function.
    /// This virtual member function can be defined in concrete parser classes
    /// to handle close element parsing.
    /// @param name Name of element
    /// @param content Text content collected between the start and end element
    /// with leading and trailing whitespace trimmed.
    virtual void end_element(const XML_Char *name, const std::string &content) {};

    /// Performs a sub-parse.
    /// This virtual member function is meant to be used in conjunction with a
    /// set of element names passed into the constructor to be parsed with a
    /// separate parser.  The parsing framework will skip the raw text
    /// associated with a sub-parse element and will pass it into this method for
    /// parsing.  Concrete parser classes should define this member function and
    /// should parse the element defined by <code>base</code> and
    /// <code>len</code> using a different (sub) parser.
    /// @param name Name of element
    /// @param base Pointer to raw element text to be parsed
    /// @param len Length of raw element text
    virtual void sub_parse(const XML_Char *name, const char *base, int len) {};

    /// Helper function to convert content to an int64_t value.
    /// This method will convert <code>content</code> to an int64_t value.
    /// @param name Name of element or attribute from which content originated
    /// @param content Content of element or attribute
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if content
    /// is empty or does not contain an ASCII value representing a valid 64-bit
    /// integer.
    static int64_t content_to_i64(const std::string &name, const std::string &content);

    /// Helper function to convert content to an int32_t value.
    /// This method will convert <code>content</code> to an int32_t value.
    /// @param name Name of element or attribute from which content originated
    /// @param content Content of element or attribute
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if content
    /// is empty or does not contain an ASCII value representing a valid 32-bit
    /// integer.
    static int32_t content_to_i32(const std::string &name, const std::string &content);

    /// Helper function to convert content to an int16_t value.
    /// This method will convert <code>content</code> to an int16_t value.
    /// @param name Name of element or attribute from which content originated
    /// @param content Content of element or attribute
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if content
    /// is empty or does not contain an ASCII value representing a valid 16-bit
    /// integer.
    static int16_t content_to_i16(const std::string &name, const std::string &content);

    /// Helper function to convert content to a boolean value.
    /// This method will convert <code>content</code> to a boolean value by
    /// doing a case insensitive match against the strings "true" and "false".
    /// If there is a match, the corresponding boolean value is returned,
    /// otherwise an exception is thrown.
    /// @param name Name of element or attribute from which content originated
    /// @param content Content of element or attribute
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if content
    /// is empty or does not case insensitively match either "true" or "false".
    static bool content_to_bool(const std::string &name, const std::string &content);

    /// Helper function to convert content to one of a set of valid text values.
    /// This helper function does a case insensitive match of <code>content</code>
    /// to each of the values in <code>valid</code>, returning the corresponding
    /// value from <code>valid</code> if a match is found.  Otherwise an exception
    /// is thrown.
    /// @param name Name of element or attribute from which content originated
    /// @param content Content of element or attribute
    /// @param valid Set of valid text values.
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if content
    /// is empty or does not match one of the values in <code>valid</code>
    static const std::string
      content_to_text(const std::string &name, const std::string &content,
                      const std::initializer_list<std::string> &valid);

  protected:

    /// eXpat parser
    XML_Parser m_parser;

    /// Pointer to buffer holding content to be parsed
    const char *m_base;

    /// Length of data at #m_base to be parsed
    int m_length;

    /// Element stack
    std::stack<std::string> m_element_stack;

  private:

    /// Determines if element should be parsed or included in a sub parse.
    /// If #m_sub_parse_toplevel >= 0, then the element is pushed onto the stack
    /// with push_element() and <i>false</i> is returned.  Otherwise,
    /// <code>name</code> is checked against the element names in
    /// #m_sub_parsers.  If there is a match, #m_sub_parse_base_offset is set to
    /// the current byte index of the parse, #m_sub_parse_toplevel is set to the
    /// current element stack size, the element is pushed onto the stack with
    /// push_element(), and <i>false</i> is returned.  Otherwise, <i>true</i> is
    /// returned signaling that the element should be parsed by the current
    /// parser.
    /// @param name Element name
    /// @return <i>true</i> if the element and its attributes should be passed
    /// to start_element(), <i>false</i> if the element is to be skipped because
    /// it is being collected for a sub-parse
    bool open_element(const XML_Char *name);

    /// Checks for and performs sub parse.
    /// If #m_sub_parse_toplevel >= 0 and we've arrived back to the sub parse
    /// toplevel (e.g. #m_sub_parse_toplevel == element stack size), then
    /// sub_parse() is called to parse the collected text with a different (sub)
    /// parser.
    /// @param name Element name
    /// @return <i>true</i> if the element should be parsed or skipped because it
    /// is part of a sub parse.
    /// @throws Exception with code set to Error::SCHEMA_PARSE_ERROR if closing
    /// tag is not well formed.
    bool close_element(const XML_Char *name);

    /// Collects text.
    /// Adds <code>len</code> characters of text from <code>s</code> to
    /// #m_collected_text.
    /// @param s Pointer to buffer of characters of text to collect
    /// @param len Number of characters to collect
    void add_text(const XML_Char *s, int len);

    /// Push element onto element stack.
    /// Pushes <code>name</code> onto #m_element_stack, sets #m_current_element
    /// to <code>name</code>, and clears #m_collected_text.
    /// @param name Element name
    void push_element(const XML_Char *name);

    /// Pops element from element stack.
    /// Pops top element from #m_element_stack and sets #m_current_element to
    /// the element on the top of the stack if it is not empty, otherwise sets
    /// it to "".
    void pop_element();

    /// Returns collected text.
    /// Returns #m_collected_text.
    /// @return Collected text.
    const std::string collected_text() { return m_collected_text; }

    /// eXpat start element handler.
    /// Calls open_element() and returns immediately if it returns
    /// <i>false</i>.  Otherwise, start_element() is called and then
    /// push_element() is called to push the element onto the element
    /// stack.
    /// @param userdata Pointer to parser
    /// @param name Name of element
    /// @param atts Attribute list of even length with each attribute name
    /// immediately followed by its corresponding value
    static void start_element_handler(void *userdata, const XML_Char *name,
                                      const XML_Char **atts);

    /// eXpat end element handler.
    /// pop_element() is called to pop the top element off the
    /// element stack.  Then close_element() is called and if it
    /// returns <i>false</i>, the function returns.  Otherwise, the collected
    /// text is obtained from the parser, leading and trailing whitespace are
    /// trimmed, and it's passed into end_element().
    /// @param userdata Pointer to parser
    /// @param name Name of element
    static void end_element_handler(void *userdata, const XML_Char *name);

    /// eXpat character data handler
    /// add_text() is called to add <code>len</code> characters
    /// starting at <code>s</code> to collected text.
    /// @param userdata Pointer to parser
    /// @param s Pointer to character buffer
    /// @param len Length of character buffer
    static void character_data_handler(void *userdata, const XML_Char *s, int len);

    /// Collected element text
    std::string m_collected_text;

    /// List of element names for which there is a sub-parser
    std::vector<std::string> m_sub_parsers;

    /// Current element being parsed
    std::string m_current_element;

    /// Toplevel element of current sub parse
    int m_sub_parse_toplevel {-1};

    /// Raw text offset (from #m_base) of beginning of sub parse
    int m_sub_parse_base_offset {};
  };

  /// @}
}

#endif // Common_XmlParser_h
