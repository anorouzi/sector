/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <index.h>
#include <util.h>

using namespace std;
using namespace cb;

CIndex::CIndex()
{
   Sync::initMutex(m_IndexLock);
}

CIndex::~CIndex()
{
   Sync::releaseMutex(m_IndexLock);
}

int CIndex::lookup(const string& filename, set<CFileAttr, CAttrComp>* filelist)
{
   int res = -1;

   Sync::enterCS(m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(filename);

   if (i !=  m_mFileList.end())
   {
      if (NULL != filelist)
         *filelist = i->second;
      res =  1;
   }

   Sync::leaveCS(m_IndexLock);

   return res;
}

int CIndex::insert(const CFileAttr& attr)
{
   Sync::enterCS(m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(attr.m_pcName);

   if (i == m_mFileList.end())
   {
      set<CFileAttr, CAttrComp> sa;
      m_mFileList[attr.m_pcName] = sa;
   }

   m_mFileList[attr.m_pcName].insert(attr);

   Sync::leaveCS(m_IndexLock);

   return 1;
}
   
int CIndex::remove(const string& filename)
{
   Sync::enterCS(m_IndexLock);
   m_mFileList.erase(filename);
   Sync::leaveCS(m_IndexLock);

   return 1;
}

void CIndex::updateNameServer(const string& filename, const Node& loc)
{
   Sync::enterCS(m_IndexLock);
   strcpy((char*)m_mFileList[filename].begin()->m_pcNameHost, loc.m_pcIP);
   const_cast<int&>(m_mFileList[filename].begin()->m_iNamePort) = loc.m_iAppPort;
   Sync::leaveCS(m_IndexLock);
}

void CIndex::removeCopy(const CFileAttr& attr)
{
   Sync::enterCS(m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(attr.m_pcName);
   if (i != m_mFileList.end())
   {
      i->second.erase(attr);

      if (0 == i->second.size())
         m_mFileList.erase(i);
   }

   Sync::leaveCS(m_IndexLock);
}

int CIndex::getFileList(map<string, set<CFileAttr, CAttrComp> >& list)
{
   list.clear();
   list = m_mFileList;
   return list.size();
}
