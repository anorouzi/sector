/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
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

#ifndef __SECTOR_FS_H__
#define __SECTOR_FS_H__

#include <stdint.h>
#include <string>
#include <vector>

using namespace std;

class SectorFS
{
public:
   SectorFS();
   ~SectorFS();

public:
   int init(const string dir);
   int create(const string& filename, const uint32_t& key, string& loc);
   int scan(vector<string>& filelist, vector<string>& dirs, const string& currdir);

private:
   static const int m_iLevel = 3;
   string m_strHomeDir;
};

#endif
