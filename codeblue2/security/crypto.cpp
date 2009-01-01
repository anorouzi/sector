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

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include "crypto.h"
#include <iostream>
using namespace std;

Crypto::Crypto():
m_iCoderType(0)
{
}

Crypto::~Crypto()
{
}

int Crypto::generateKey(unsigned char key[16], unsigned char iv[8])
{
   int fd;
   if ((fd = open("/dev/random", O_RDONLY)) == -1)
   {
      perror("open error");
      return -1;
   }

   if ((read (fd, key, 16)) == -1)
   {
      perror("read key error");
      return -1;
   }

   if ((read (fd, iv, 8)) == -1)
   {
      perror("read iv error");
      return -1;
   }

   //for (int i = 0; i < 16; i++)
   //   printf("%d \t", key[i]);

   close (fd);
   return 0;
}

int Crypto::initEnc(unsigned char key[16], unsigned char iv[8])
{
   memcpy(m_pcKey, key, 16);
   memcpy(m_pcIV, iv, 8);

   EVP_CIPHER_CTX_init(&m_CTX);
   EVP_EncryptInit(&m_CTX, EVP_bf_cbc(), m_pcKey, m_pcIV);

   m_iCoderType = 1;

   return 0;
}

int Crypto::initDec(unsigned char key[16], unsigned char iv[8])
{
   memcpy(m_pcKey, key, 16);
   memcpy(m_pcIV, iv, 8);

   EVP_CIPHER_CTX_init(&m_CTX);
   EVP_DecryptInit(&m_CTX, EVP_bf_cbc(), m_pcKey, m_pcIV);

   m_iCoderType = -1;

   return 0;
}

int Crypto::release()
{
   EVP_CIPHER_CTX_cleanup(&m_CTX);
   m_iCoderType = 0;
   return 0;
}

int Crypto::encrypt(unsigned char* input, int insize, unsigned char* output, int& outsize)
{
   if (1 != m_iCoderType)
      return -1;

   unsigned char* ip = input;
   unsigned char* op = output;

   for (int ts = insize; ts > 0; )
   {
      int unitsize = (ts < g_iEncBlockSize) ? ts : g_iEncBlockSize;

      int len;
      if (EVP_EncryptUpdate(&m_CTX, op, &len, ip, unitsize) != 1)
      {
         printf ("error in encrypt update\n");
         return 0;
      }

      ip += unitsize;
      op += len;

      if (EVP_EncryptFinal(&m_CTX, op, &len) != 1)
      {
          printf ("error in encrypt final\n");
          return 0;
      }

      op += len;
      ts -= unitsize;
   }

   outsize = op  - output;
   return 1;
}

int Crypto::decrypt(unsigned char* input, int insize, unsigned char* output, int& outsize)
{
   if (-1 != m_iCoderType)
      return -1;

   unsigned char* ip = input;
   unsigned char* op = output;

   for (int ts = insize; ts > 0; )
   {
      int unitsize = (ts < g_iDecBlockSize) ? ts : g_iDecBlockSize;

      int len;
      if (EVP_DecryptUpdate(&m_CTX, op, &len, ip, unitsize) != 1)
      {
         printf("error in decrypt update\n");
         return 0;
      }

      ip += unitsize;
      op += len;

      if (EVP_DecryptFinal(&m_CTX, op, &len) != 1)
      {
         printf("error in decrypt final\n");
         return 0;
      }

      op += len;
      ts -= unitsize;
   }

   outsize = op - output;
   return 1;
}

