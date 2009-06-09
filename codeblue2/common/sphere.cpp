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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/30/2008
*****************************************************************************/

#include <sphere.h>
#include <cstring>

int SOutput::resizeResBuf(const int64_t& newsize)
{
   char* tmp = NULL;

   try
   {
      tmp = new char[newsize];
   }
   catch (...)
   {
      return -1;
   }

   memcpy(tmp, m_pcResult, m_iResSize);
   delete [] m_pcResult;
   m_pcResult = tmp;

   m_iBufSize = newsize;

   return newsize;
}

int SOutput::resizeIdxBuf(const int64_t& newsize)
{
   int64_t* tmp1 = NULL;
   int* tmp2 = NULL;

   try
   {
      tmp1 = new int64_t[newsize];
      tmp2 = new int[newsize];
   }
   catch (...)
   {
      return -1;
   }

   memcpy(tmp1, m_pllIndex, m_iRows * 8);
   delete [] m_pllIndex;
   m_pllIndex = tmp1;

   memcpy(tmp2, m_piBucketID, m_iRows * sizeof(int));
   delete [] m_piBucketID;
   m_piBucketID = tmp2;

   m_iIndSize = newsize;

   return newsize;
}

