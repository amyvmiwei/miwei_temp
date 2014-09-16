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
/// Definitions for Crypto.
/// This file contains type definitions for Crypto, a static class that
/// provides methods for encryption and decryption.

#include <Common/Compat.h>

#include "Crypto.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

extern "C" {
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/err.h>
}

using namespace Hypertable;

namespace {

  RSA *create_rsa(unsigned char *key, bool is_public) {
    RSA *rsa {};
    BIO *keybio = BIO_new_mem_buf(key, -1);

    HT_ASSERT(keybio);

    if (is_public)
      rsa = PEM_read_bio_RSA_PUBKEY(keybio, &rsa, NULL, NULL);
    else
      rsa = PEM_read_bio_RSAPrivateKey(keybio, &rsa, NULL, NULL);

    return rsa;
  }

}

const string Crypto::rsa_encrypt(const char *public_key, const string &message) {
  char error_buf[256];
  EVP_PKEY *evpPubKey {};

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();

  BIO *bio = BIO_new_mem_buf((void *)public_key, -1);

  PEM_read_bio_PUBKEY(bio, &evpPubKey, NULL, NULL);

  BIO_free_all(bio);

  size_t encMsgLen = 0;
  int blockLen  = 0;

  unsigned char *iv = new unsigned char [EVP_MAX_IV_LENGTH];
  int ek_len;
  unsigned char *ek[1];
  ek[0] = new unsigned char [EVP_PKEY_size(evpPubKey)];

  unsigned char *encMsg = new unsigned char [message.length() + EVP_MAX_IV_LENGTH];

  if (!EVP_SealInit(ctx, EVP_aes_256_cbc(), ek, &ek_len, iv, &evpPubKey, 1)) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_SealInit() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }

  if (!EVP_SealUpdate(ctx, encMsg + encMsgLen, &blockLen, (const unsigned char*)message.c_str(), (int)message.length())) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_SealUpdate() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }
  encMsgLen += blockLen;

  if(!EVP_SealFinal(ctx, encMsg + encMsgLen, &blockLen)) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_SealFinal() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }
  encMsgLen += blockLen;

  string encrypted_message;
  encrypted_message.reserve(2 + ek_len + EVP_MAX_IV_LENGTH + encMsgLen + 1);
  uint8_t buf[2];
  uint8_t *ptr = buf;
  Serialization::encode_i16(&ptr, (uint16_t)ek_len);
  encrypted_message.append((const char *)buf, 2);
  encrypted_message.append((const char *)ek[0], ek_len);
  encrypted_message.append((const char *)iv, EVP_MAX_IV_LENGTH);
  encrypted_message.append((const char *)encMsg, encMsgLen);

  delete [] iv;
  delete [] ek[0];
  delete [] encMsg;
  EVP_CIPHER_CTX_free(ctx);

  return encrypted_message;
}

const string Crypto::rsa_decrypt(const char *private_key, const string &message) {
  char error_buf[256];
  EVP_PKEY *evpPriKey {};
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();

  BIO *bio = BIO_new_mem_buf((void *)private_key, -1);

  PEM_read_bio_PrivateKey(bio, &evpPriKey, nullptr, nullptr);

  BIO_free_all(bio);

  unsigned char *ek {};
  int ek_len {};
  unsigned char *iv {};
  unsigned char *ciphertext {};
  int ciphertext_len {};

  const uint8_t *buf = (const uint8_t *)message.c_str();
  size_t remain = message.length();
  ek_len = Serialization::decode_i16(&buf, &remain);

  ek = (unsigned char *)buf;
  buf += ek_len;
  remain -= ek_len;

  iv = (unsigned char *)buf;
  buf += EVP_MAX_IV_LENGTH;
  remain -= EVP_MAX_IV_LENGTH;

  ciphertext = (unsigned char *)buf;
  ciphertext_len = (int)remain;

  if (EVP_OpenInit(ctx, EVP_aes_256_cbc(), ek, ek_len, iv, evpPriKey) != 1) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_OpenInit() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }

  int len {};
  int plaintext_len {};
  unsigned char *plaintext = new unsigned char [ message.length() ];
  if (EVP_OpenUpdate(ctx, plaintext, &len, ciphertext, ciphertext_len) != 1) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_OpenUpdate() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }
  plaintext_len = len;

  if (EVP_OpenFinal(ctx, plaintext + len, &len) != 1) {
    ERR_load_crypto_strings();
    HT_FATALF("EVP_OpenFinal() - %s", ERR_error_string(ERR_get_error(), error_buf));
  }
  plaintext_len += len;

  HT_ASSERT(plaintext_len <= (int)message.length());

  string decrypted_message;
  decrypted_message.reserve(plaintext_len+1);
  decrypted_message.append((const char *)plaintext, plaintext_len);

  delete [] plaintext;
  EVP_CIPHER_CTX_free(ctx);

  return decrypted_message;
}


const string Crypto::rsa_signature_encrypt(const char *key, bool key_is_public,
                                           const string &message) {
  RSA *rsa = create_rsa((unsigned char *)key, key_is_public ? 1 : 0);
  HT_ASSERT(rsa);
  int buf_len = message.length() + RSA_size(rsa);
  unsigned char *buf = new unsigned char [ buf_len ];
  int len;
  if (key_is_public)
    len = RSA_public_encrypt(message.length(), (const unsigned char *)message.c_str(), buf, rsa, RSA_PKCS1_PADDING);
  else
    len = RSA_private_encrypt(message.length(), (const unsigned char *)message.c_str(), buf, rsa, RSA_PKCS1_PADDING);
  HT_ASSERT(len < buf_len);
  if (len < 0) {
    char ebuf[256];
    ERR_load_crypto_strings();
    HT_FATALF("RSA_public_encrypt() - %s", ERR_error_string(ERR_get_error(), ebuf));
  }
  string encrypted_message;
  encrypted_message.reserve(len+1);
  encrypted_message.append((const char *)buf, len);
  free(rsa);
  delete [] buf;
  return encrypted_message;
}

const string Crypto::rsa_signature_decrypt(const char *key, bool key_is_public,
                                           const string &signature) {
  RSA *rsa = create_rsa((unsigned char *)key, key_is_public ? 1 : 0);
  HT_ASSERT(rsa);
  int buf_len = signature.length() + RSA_size(rsa);
  unsigned char *buf = new unsigned char [ buf_len ];
  int len;
  if (key_is_public)
    len = RSA_public_decrypt(signature.length(),
                             (const unsigned char *)signature.c_str(),
                             buf, rsa, RSA_PKCS1_PADDING);
  else
    len = RSA_private_decrypt(signature.length(),
                              (const unsigned char *)signature.c_str(),
                              buf, rsa, RSA_PKCS1_PADDING);
  HT_ASSERT(len < buf_len);
  if (len < 0) {
    char ebuf[256];
    ERR_load_crypto_strings();
    HT_FATALF("RSA_public_decrypt() - %s", ERR_error_string(ERR_get_error(), ebuf));
  }
  string decrypted_message;
  decrypted_message.reserve(len+1);
  decrypted_message.append((const char *)buf, len);
  free(rsa);
  delete [] buf;
  return decrypted_message;
}
