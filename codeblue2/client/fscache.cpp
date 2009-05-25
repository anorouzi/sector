/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 05/23/2009
*****************************************************************************/

#include "fscache.h"

using namespace std;

void StatCache::insert(const string& path)
{
   map<string, StatRec>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
   {
      StatRec r;
      r.m_iCount = 1;
      r.m_bChange = false;
      m_mOpenedFiles[path] = r;
   }
   else
   {
      s->second.m_iCount ++;
   }
}

void StatCache::update(const string& path, const int64_t& ts, const int64_t& size)
{
   map<string, StatRec>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return;

   s->second.m_llTimeStamp = ts;
   s->second.m_llSize = size;

   s->second.m_bChange = true;
}

int StatCache::stat(const string& path, SNode& attr)
{
   map<string, StatRec>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return -1;

   if (!s->second.m_bChange)
      return 0;

   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_llSize = s->second.m_llSize;
   return 1;
}

void StatCache::remove(const string& path)
{
   map<string, StatRec>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return;

   if (-- s->second.m_iCount == 0)
      m_mOpenedFiles.erase(s);
}
