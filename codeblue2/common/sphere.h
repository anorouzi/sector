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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/29/2008
*****************************************************************************/

#ifndef __SPHERE_H__
#define __SPHERE_H__

#include <stdint.h>
#include <set>
#include <string>

struct SInput
{
   char* m_pcUnit;		// input data
   int m_iRows;			// number of records/rows
   int64_t* m_pllIndex;		// record index

   char* m_pcParam;		// parameter, NULL is no parameter
   int m_iPSize;		// size of the parameter, 0 if no parameter
};

struct SOutput
{
   char* m_pcResult;		// buffer to store the result
   int m_iBufSize;		// size of the buffer
   int m_iResSize;		// size of the result

   int64_t* m_pllIndex;		// record index of the result
   int m_iIndSize;		// size of the index structure
   int m_iRows;			// number of records/rows

   int* m_piBucketID;		// bucket ID
};

struct SFile
{
   std::string m_strHomeDir;		// Sector data home directory
   std::set<std::string> m_sstrFiles; 	// list of modified files
};

#endif
