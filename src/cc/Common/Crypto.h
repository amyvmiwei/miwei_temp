/* -*- c++ -*-
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
/// Declarations for Crypto.
/// This file contains type declarations for Crypto, a static class that
/// provides methods for encryption and decryption.

#ifndef Common_Crypto_h
#define Common_Crypto_h

#include <string>

namespace Hypertable {

  using namespace std;

  /// @addtogroup Common
  /// @{

  /// Provides cryptographic functions
  class Crypto {
  public:

    /// Encrypts a message with RSA algorithm.
    /// @param public_key NULL terminated RSA public key string
    /// @param message Message to encrypt
    /// @return Encrypted message
    static const string rsa_encrypt(const char *public_key, const string &message);

    /// Decrypts a message with RSA algorithm.
    /// @param private_key NULL terminated RSA private key string
    /// @param message Message to decrypt
    /// @return Decrypted message
    static const string rsa_decrypt(const char *private_key, const string &message);

    /// Computes RSA signature for message.
    /// @param key NULL terminated key string
    /// @param key_is_public Flag indicating if <code>key</code> is public key
    /// @param message Message to encrypt RSA signature
    /// @return Encrypted message
    static const string rsa_signature_encrypt(const char *key,
                                              bool key_is_public,
                                              const string &message);

    /// Recovers message from RSA signature.
    /// @param key NULL terminated key string
    /// @param key_is_public Flag indicating if <code>key</code> is public key
    /// @param signature RSA signature of message to decrypt
    /// @return Decrypted message
    static const string rsa_signature_decrypt(const char *key,
                                              bool key_is_public,
                                              const string &signature);

  };

  /// @}
}

#endif // Common_Crypto_h
