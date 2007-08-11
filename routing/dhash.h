/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

SECTOR: A Distributed Storage and Computing Infrastructure

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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __DHASH_H__
#define __DHASH_H__

#include <openssl/sha.h>
#include <math.h>
#include <string>

namespace cb
{

class DHash
{
public:
   DHash(): m_im(32) {}
   DHash(const int m): m_im(m) {}
   ~DHash() {}

   unsigned int hash(const char* str)
   {
      unsigned char res[SHA_DIGEST_LENGTH];

      SHA1((const unsigned char*)str, strlen(str), res);

      return *(unsigned int*)(res + SHA_DIGEST_LENGTH - 4);
   }

   static unsigned int hash(const char* str, int m)
   {
      unsigned char res[SHA_DIGEST_LENGTH];

      SHA1((const unsigned char*)str, strlen(str), res);

      if (m >= 32)
         return *(unsigned int*)(res + SHA_DIGEST_LENGTH - 4);

      unsigned int mask = 1;
      mask = (mask << m) - 1;

      return (*(unsigned int*)(res + SHA_DIGEST_LENGTH - 4)) & mask;
   }

private:
   unsigned int m_im;
};

}; // namespace

#endif
