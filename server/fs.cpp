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


#include "fs.h"
#include <cstdio>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <iostream>

using namespace std;

SectorFS::SectorFS():
m_strHomeDir("")
{
}

SectorFS::~SectorFS()
{
}

int SectorFS::init(const string dir)
{
   m_strHomeDir = dir;
   return 1;
}

int SectorFS::locate(const string& filename, const uint32_t& key, string& loc)
{
   // loc input: home dir directory

   char keystr[32];
   sprintf(keystr, "%d", key);

   for (int i = 0; i < m_iLevel; ++ i)
   {
      char tmp[2];
      tmp[0] = keystr[i * 2];
      tmp[1] = keystr[i * 2 + 1];
      loc += tmp;
      loc += "/";
   }

   loc += filename;

   return 1;
}

int SectorFS::create(const string& filename, const uint32_t& key, string& loc)
{
   // loc input: home dir directory

   char keystr[32];
   sprintf(keystr, "%d", key);

   for (int i = 0; i < m_iLevel; ++ i)
   {
      char tmp[2];
      tmp[0] = keystr[i * 2];
      tmp[1] = keystr[i * 2 + 1];

      loc += tmp;
      mkdir(loc.c_str(), S_ISVTX);
      loc += "/";
   }

   loc += filename;

   return 1;
}

int SectorFS::scan(vector<string>& filelist, vector<string>& dirs, const string& currdir)
{
   cout << "scaning " << currdir << endl;

   dirent **namelist;
   int n = scandir((m_strHomeDir + currdir).c_str(), &namelist, 0, alphasort);

   if (n < 0)
      return -1;

   for (int i = 2; i < n; ++ i)
   {
      struct stat s;
      stat((m_strHomeDir + namelist[i]->d_name).c_str(), &s);

      if (S_ISDIR(s.st_mode))
         scan(filelist, dirs, currdir + namelist[i]->d_name + "/");
      else
      {
         filelist.insert(filelist.end(), namelist[i]->d_name);
         dirs.insert(dirs.end(), currdir);
      }

      free(namelist[i]);
   }
   free(namelist);

   return filelist.size();
}
