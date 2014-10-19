/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
/// Definitions for Base64.
/// This file contains type definitions for Base64, a static class that
/// provides base64 encoding and decoding functions.

#include <Common/Compat.h>

#include "Base64.h"

#if defined(__APPLE__)
#include <Common/fmemopen.h>
#endif

#include <string>

extern "C" {
#include <openssl/bio.h>
#include <openssl/evp.h>

#include <math.h>
#include <stdio.h>
#include <string.h>
}

using namespace Hypertable;

namespace {
  int decode_length(const string message) {
    int padding = 0;
    
    // Check for trailing '=''s as padding
    if (message[message.length()-1] == '=' && message[message.length()-2] == '=')
      padding = 2;
    else if (message[message.length()-1] == '=')
      padding = 1;
     
    return (int)message.length()*0.75 - padding;
  }

}

const string Base64::encode(const string &message, bool newlines) {
  BIO *bio;
  BIO *b64;
  FILE* stream;

  int encoded_length = (4*ceil((double)message.length()/3)) + 64 + 1;
  char *buffer = new char [encoded_length];
     
  stream = fmemopen(buffer, encoded_length, "w");
  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new_fp(stream, BIO_NOCLOSE);
  bio = BIO_push(b64, bio);
  if (!newlines)
    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(bio, message.c_str(), message.length());
  (void)BIO_flush(bio);
  int buffer_length = BIO_tell(bio);
  BIO_free_all(bio);
  fclose(stream);

  string encoded_message(buffer, buffer_length);
  delete [] buffer;

  return encoded_message;
}

const string Base64::decode(const string &message, bool newlines) {
  BIO *bio;
  BIO *b64;
  int decoded_length = decode_length(message);

  unsigned char *buffer = new unsigned char [decoded_length+1];
  FILE* stream = fmemopen((char*)message.c_str(), message.length(), "r");
     
  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new_fp(stream, BIO_NOCLOSE);
  bio = BIO_push(b64, bio);
  if (!newlines)
    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  decoded_length = BIO_read(bio, buffer, message.length());
  buffer[decoded_length] = '\0';
     
  BIO_free_all(bio);
  fclose(stream);

  string decoded_message((const char *)buffer, decoded_length);
  delete buffer;

  return decoded_message;
}
