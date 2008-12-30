/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 12/29/2008
*****************************************************************************/

#ifndef __CRYPTO_H__
#define __CRYPTO_H__

#include <openssl/evp.h>

class Crypto
{
public:
   Crypto();
   ~Crypto();

public:
   static int generateKey(unsigned char key[16], unsigned char iv[8]);

   int initEnc(unsigned char key[16], unsigned char iv[8]);
   int initDec(unsigned char key[16], unsigned char iv[8]);
   int release();

   int encrypt(unsigned char* input, int insize, unsigned char* output, int& outsize);
   int decrypt(unsigned char* input, int insize, unsigned char* output, int& outsize);

private:
   unsigned char m_pcKey[16];
   unsigned char m_pcIV[8];
   EVP_CIPHER_CTX m_CTX;
   int m_iCoderType;		// 1: encoder, -1:decoder

   static const int m_giEncBlockSize = 1024;
   static const int m_giDecBlockSize = 1032;
};

#endif
