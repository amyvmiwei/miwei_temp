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

#include <Common/Compat.h>
#include <Common/Crypto.h>
#include <Common/Logger.h>

#include <fstream>
#include <iostream>

using namespace Hypertable;
using namespace std;

namespace {

  const char *public_key =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwunMXfTfWI08An+apWX2\n"
    "ePovpmP9SfCsSx4vONsVgM0ypU5c2JfonOQrL3JM7V3twUenqzLePB26Cu5YnGDg\n"
    "nmr9ktu34yD4eCDnvYhAte1U/Y1FMeekacuyy3VFZ2/yQSxjsw+QdZwxRJ0GHQLE\n"
    "RscrRHnrX4rY2wubT3vSEMEUsPi3org5PBxbR62eh3FTnp/MN7uuw+0u8ccjP+JV\n"
    "pigkqKqTghnyv7kmdeKY39FPduB6uL+nLAQBHvPPEJqYBqgam8NeBzg4l5NDAnBj\n"
    "KT4T7Dx9kmHR4d2nP8HVyn9NtdOIY57w1tieM1WAI/h4HMKsuYNTL+oqNZOkZOJw\n"
    "5QIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

  const char *private_key =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDC6cxd9N9YjTwC\n"
    "f5qlZfZ4+i+mY/1J8KxLHi842xWAzTKlTlzYl+ic5CsvckztXe3BR6erMt48HboK\n"
    "7licYOCeav2S27fjIPh4IOe9iEC17VT9jUUx56Rpy7LLdUVnb/JBLGOzD5B1nDFE\n"
    "nQYdAsRGxytEeetfitjbC5tPe9IQwRSw+LeiuDk8HFtHrZ6HcVOen8w3u67D7S7x\n"
    "xyM/4lWmKCSoqpOCGfK/uSZ14pjf0U924Hq4v6csBAEe888QmpgGqBqbw14HODiX\n"
    "k0MCcGMpPhPsPH2SYdHh3ac/wdXKf02104hjnvDW2J4zVYAj+Hgcwqy5g1Mv6io1\n"
    "k6Rk4nDlAgMBAAECggEAc9UDebbDYFmWlxmEKtT8c4qi9KwpY16e1YlboNs53TCU\n"
    "734xWRp1x38lVu4DX3DZkWpm7yusvGciH8rjaBN+iUal6TegAV+fqaxMa+tkIXmo\n"
    "b4Ib4/t4TtMaLyVDGKSDgE3D9i7Ct9IZyV/TuTvirvk/8lLOGRpno00zgDnibcd2\n"
    "U+x6JZPLZ1q1Pap5peR2ZFnslVOVIJ98ZBnlLaKIZ6yHzrPJs8xBHnC8gQFtFFxh\n"
    "KIo+vqT1I9QfoVM7855dnkRbqE1CV2UXJmu8PupJNiO8UfXIh3PNvOO5i49G7WnZ\n"
    "gFySuxZxR7KHRsZLB4s8X5hMt7jWMIvBKUZtpgTlxQKBgQD0amfaz0yVxk4VA2L5\n"
    "Fgwa7t7gY+6jyHYwBT7NIea/zkmI4Bm7l9QnLXQ3k8PjBWhBN5yxVTQQ96hCyLAS\n"
    "OVIJl48E4YtPi7EU9HkxuhxSxx6pvlg9bCqVHhUCjLmoUphcchBlL+wdO0BSanmf\n"
    "W6rT6sZGDirh6xe/OSLNdEe/IwKBgQDMJsKeuTUkQFH7Jc0XuCWWBK6EYsbJ+T7l\n"
    "FNziLa5aFh+KVkvWR/plfn+fx0u93fn8155XbSF1veCvy2JC0fGsJYGmMsnoclev\n"
    "VJnPYIN+gv6r5FNU58a9Fekp896ZlxsUj31dN8DCToBD424QSxyMRUDvn/vPKy/+\n"
    "WnUlPCNUVwKBgHXT3iFEEl2Z7gZ/TXVbRZauVU/DnUXj2+YmgTIPB9irsBOGRaGD\n"
    "/kisxvwMBkEpWCsojieHNcSAP+OCMC945q2tHMtzl99PLp01hj+Mwx6803FtS8jA\n"
    "nn6os+Q4xz/4IW9fpbvCMIefCtEwd1V1sZLb+Z3IMrAaz6Xl+3skdXoVAoGAe5ow\n"
    "FSdvTE6BFDzTyrucThLxkoe3ccutT8ds4KfluvdQC1KqQcd8i/ylyphAfGksFPB7\n"
    "FtWk/4OByQ2rpWc1GhPvRi/T3R91hVsEcPEvS+aFyI7KAUr/IbZlYBgduwIozC8A\n"
    "KPM7iqk6sV++YLEJTMgVQ7+qFslAGpCsAmpBvasCgYEAgHC5I8RFIl15Z1agHHOa\n"
    "VKxDQghFLg+hTi9LobbrkpeNW0q+tIoeeUAftFIwq9kS88SqqvXNhvufZ9o+/kyN\n"
    "LSE6wT606Wa2DTgTqpYy1uTv9wA+eyqipvPojDIFOBbtomDve5Ny4upctHmKNcRs\n"
    "OAAgplqe8GyLvHiFphCDgxU=\n"
    "-----END PRIVATE KEY-----\n";

  const char *really_long_message =
    "\"Well, Prince, so Genoa and Lucca are now just family estates of the\n"
    "Buonapartes. But I warn you, if you don't tell me that this means war,\n"
    "if you still try to defend the infamies and horrors perpetrated by that\n"
    "Antichrist--I really believe he is Antichrist--I will have nothing more\n"
    "to do with you and you are no longer my friend, no longer my 'faithful\n"
    "slave,' as you call yourself! But how do you do? I see I have frightened\n"
    "you--sit down and tell me all the news.\"\n"
    "\n"
    "It was in July, 1805, and the speaker was the well-known Anna Pavlovna\n"
    "Scherer, maid of honor and favorite of the Empress Marya Fedorovna. With\n"
    "these words she greeted Prince Vasili Kuragin, a man of high rank and\n"
    "importance, who was the first to arrive at her reception. Anna Pavlovna\n"
    "had had a cough for some days. She was, as she said, suffering from la\n"
    "grippe; grippe being then a new word in St. Petersburg, used only by the\n"
    "elite.\n"
    "\n"
    "All her invitations without exception, written in French, and delivered\n"
    "by a scarlet-liveried footman that morning, ran as follows:\n"
    "\n"
    "\"If you have nothing better to do, Count (or Prince), and if the\n"
    "prospect of spending an evening with a poor invalid is not too terrible,\n"
    "I shall be very charmed to see you tonight between 7 and 10--Annette\n"
    "Scherer.\"\n"
    "\n"
    "\"Heavens! what a virulent attack!\" replied the prince, not in the least\n"
    "disconcerted by this reception. He had just entered, wearing an\n"
    "embroidered court uniform, knee breeches, and shoes, and had stars on\n"
    "his breast and a serene expression on his flat face. He spoke in that\n"
    "refined French in which our grandfathers not only spoke but thought, and\n"
    "with the gentle, patronizing intonation natural to a man of importance\n"
    "who had grown old in society and at court. He went up to Anna Pavlovna,\n"
    "kissed her hand, presenting to her his bald, scented, and shining head,\n"
    "and complacently seated himself on the sofa.\n";

}


int main(int argc, char **argv) {

  //
  // RSA signature functions
  //
  string original_message("Hello, World!");
  string encrypted_message = Crypto::rsa_signature_encrypt(private_key, false, original_message);
  string decrypted_message = Crypto::rsa_signature_decrypt(public_key, true, encrypted_message);

  HT_ASSERT(original_message.compare(decrypted_message) == 0);

  encrypted_message = Crypto::rsa_signature_encrypt(public_key, true, original_message);
  decrypted_message = Crypto::rsa_signature_decrypt(private_key, false, encrypted_message);

  HT_ASSERT(original_message.compare(decrypted_message) == 0);

  //
  // RSA message encrypt/decrypt functions
  //
  encrypted_message = Crypto::rsa_encrypt(public_key, really_long_message);
  decrypted_message = Crypto::rsa_decrypt(private_key, encrypted_message);

  HT_ASSERT(decrypted_message.compare(really_long_message) == 0);

  return 0;
}
