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

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 12/13/2007
*****************************************************************************/


#include <index.h>
#include <iostream>
#include <common.h>

using namespace std;
using namespace cb;


LocalFileIndex::LocalFileIndex()
{
   #ifndef WIN32
      pthread_mutex_init(&m_IndexLock, NULL);
   #else
      m_IndexLock = CreateMutex(NULL, false, NULL);
   #endif
}

LocalFileIndex::~LocalFileIndex()
{
   #ifndef WIN32
      pthread_mutex_destroy(&m_IndexLock);
   #else
      CloseHandle(m_IndexLock);
   #endif
}

int LocalFileIndex::lookup(const string& filename, string& dir, CFileAttr* attr)
{
   CGuard indexg(m_IndexLock);

   map<string, LocalIndexInfo>::iterator i = m_mFileIndex.find(filename);

   if (i == m_mFileIndex.end())
      return -1;

   dir = i->second.m_strDir;

   if (NULL != attr)
      *attr = i->second.m_Attr;

   return 1;
}

int LocalFileIndex::insert(const CFileAttr& attr, const string& dir, const Node* n)
{
   CGuard indexg(m_IndexLock);

   LocalIndexInfo lii;
   lii.m_Attr = attr;
   if (NULL == n)
   {
      strcpy(lii.m_Loc.m_pcIP, "");
      lii.m_Loc.m_iAppPort = 0;
   }
   else
      lii.m_Loc = *n;
   lii.m_strDir = dir;

   m_mFileIndex[attr.m_pcName] = lii;

   map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.find(lii.m_Loc);
   if (i == m_mLocIndex.end())
   {
      set<string> fl;
      i = m_mLocIndex.insert(m_mLocIndex.begin(), pair<Node, set<string> >(lii.m_Loc, fl));
   }
   i->second.insert(attr.m_pcName);

   return 1;
}

void LocalFileIndex::remove(const string& filename)
{
   CGuard indexg(m_IndexLock);

   map<string, LocalIndexInfo>::iterator i = m_mFileIndex.find(filename);

   if (i == m_mFileIndex.end())
      return;

   Node& n = i->second.m_Loc;
   m_mLocIndex[n].erase(filename);
   if (m_mLocIndex[n].empty())
      m_mLocIndex.erase(n);

   m_mFileIndex.erase(i);
}

void LocalFileIndex::updateNameServer(const string& filename, const Node& loc)
{
   CGuard indexg(m_IndexLock);

   map<string, LocalIndexInfo>::iterator i = m_mFileIndex.find(filename);

   if (i == m_mFileIndex.end())
      return;

   Node& n = i->second.m_Loc;
   m_mLocIndex[n].erase(filename);
   if (m_mLocIndex[n].empty())
      m_mLocIndex.erase(n);

   i->second.m_Loc = loc;
   map<Node, set<string>, NodeComp>::iterator l = m_mLocIndex.find(loc);
   if (l == m_mLocIndex.end())
   {
      set<string> fl;
      l = m_mLocIndex.insert(m_mLocIndex.begin(), pair<Node, set<string> >(loc, fl));
   }
   l->second.insert(filename);
}

int LocalFileIndex::getLocIndex(vector<Node>& li)
{
   CGuard indexg(m_IndexLock);

   li.clear();
   for (map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.begin(); i != m_mLocIndex.end(); ++ i)
      li.insert(li.end(), i->first);

   return li.size();
}

int LocalFileIndex::getFileList(set<string>& fl)
{
   CGuard indexg(m_IndexLock);

   fl.clear();
   for (map<string, LocalIndexInfo>::iterator i = m_mFileIndex.begin(); i != m_mFileIndex.end(); ++ i)
      fl.insert(fl.end(), i->first);

   return fl.size();
}

int LocalFileIndex::getFileList(vector<string>& fl, const Node& n)
{
   CGuard indexg(m_IndexLock);

   fl.clear();
   for (set<string>::iterator i = m_mLocIndex[n].begin(); i != m_mLocIndex[n].end(); ++ i)
      fl.insert(fl.end(), *i);

   return fl.size();
}


RemoteFileIndex::RemoteFileIndex()
{
   #ifndef WIN32
      pthread_mutex_init(&m_IndexLock, NULL);
   #else
      m_IndexLock = CreateMutex(NULL, false, NULL);
   #endif
}

RemoteFileIndex::~RemoteFileIndex()
{
   #ifndef WIN32
      pthread_mutex_destroy(&m_IndexLock);
   #else
      CloseHandle(m_IndexLock);
   #endif
}

int RemoteFileIndex::lookup(const string& filename, CFileAttr* attr, set<Node, NodeComp>* nl)
{
   CGuard indexg(m_IndexLock);

   map<string, RemoteIndexInfo>::iterator i = m_mFileIndex.find(filename);

   if (i == m_mFileIndex.end())
      return -1;

   if (NULL != attr)
      *attr = i->second.m_Attr;

   if (NULL != nl)
   {
      nl->clear();
      for (set<Node, NodeComp>::iterator j = i->second.m_sLocInfo.begin(); j != i->second.m_sLocInfo.end(); ++ j)
         nl->insert(*j);
   }

   return i->second.m_sLocInfo.size();
}

bool RemoteFileIndex::check(const string& filename, const Node& n)
{
   CGuard indexg(m_IndexLock);

   map<string, RemoteIndexInfo>::iterator i = m_mFileIndex.find(filename);
   if (i == m_mFileIndex.end())
      return false;

   return (i->second.m_sLocInfo.find(n) != i->second.m_sLocInfo.end());
}

int RemoteFileIndex::insert(const CFileAttr& attr, const Node& n)
{
   CGuard indexg(m_IndexLock);

   map<string, RemoteIndexInfo>::iterator i = m_mFileIndex.find(attr.m_pcName);

   if (i == m_mFileIndex.end())
   {
      RemoteIndexInfo rii;
      rii.m_Attr = attr;
      rii.m_sLocInfo.insert(n);
      m_mFileIndex[attr.m_pcName] = rii;
   }
   else
      i->second.m_sLocInfo.insert(n);

   map<Node, RemoteNodeInfo, NodeComp>::iterator j = m_mLocIndex.find(n);

   if (j == m_mLocIndex.end())
   {
      RemoteNodeInfo rni;
      rni.m_sFileList.insert(attr.m_pcName);
      rni.m_ullTimeStamp = 0;
      m_mLocIndex[n] = rni;
   }
   else
   {
      j->second.m_sFileList.insert(attr.m_pcName);
      j->second.m_ullTimeStamp = 0;
   }

   return 1;
}

void RemoteFileIndex::remove(const string& filename)
{
   CGuard indexg(m_IndexLock);

   map<string, RemoteIndexInfo>::iterator i = m_mFileIndex.find(filename);

   if (i == m_mFileIndex.end())
      return;

   for (set<Node, NodeComp>::iterator j = i->second.m_sLocInfo.begin(); j != i->second.m_sLocInfo.end(); ++ j)
   {
      m_mLocIndex[*j].m_sFileList.erase(filename);
      if (m_mLocIndex[*j].m_sFileList.empty())
         m_mLocIndex.erase(*j);
   }

   m_mFileIndex.erase(i);
}

void RemoteFileIndex::remove(const Node& n)
{
   CGuard indexg(m_IndexLock);

   map<Node, RemoteNodeInfo, NodeComp>::iterator i = m_mLocIndex.find(n);

   if (i == m_mLocIndex.end())
      return;

   for (set<string>::iterator f = i->second.m_sFileList.begin(); f != i->second.m_sFileList.end(); ++ f)
   {
      m_mFileIndex[*f].m_sLocInfo.erase(n);
      if (m_mFileIndex[*f].m_sLocInfo.empty())
         m_mFileIndex.erase(*f);
   }

   m_mLocIndex.erase(i);
}

void RemoteFileIndex::removeCopy(const string& filename, const Node& n)
{
   CGuard indexg(m_IndexLock);

   map<Node, RemoteNodeInfo, NodeComp>::iterator i = m_mLocIndex.find(n);
   if (i == m_mLocIndex.end())
      return;

   i->second.m_sFileList.erase(filename);
   if (i->second.m_sFileList.empty())
      m_mLocIndex.erase(i);

   map<string, RemoteIndexInfo>::iterator j = m_mFileIndex.find(filename);
   if (j == m_mFileIndex.end())
      return;

   j->second.m_sLocInfo.erase(n);
   if (j->second.m_sLocInfo.empty())
      m_mFileIndex.erase(j);
}

int RemoteFileIndex::getFileList(vector<string>& fl, const Node& n)
{
   CGuard indexg(m_IndexLock);

   fl.clear();
   for (set<string>::iterator i = m_mLocIndex[n].m_sFileList.begin(); i != m_mLocIndex[n].m_sFileList.end(); ++ i)
      fl.insert(fl.end(), *i);

   return fl.size();   
}

int RemoteFileIndex::getReplicaInfo(map<string, int>& ri, const unsigned int& num)
{
   CGuard indexg(m_IndexLock);

   ri.clear();

   for (map<string, RemoteIndexInfo>::const_iterator i = m_mFileIndex.begin(); i != m_mFileIndex.end(); ++ i)
   {
      if (i->second.m_sLocInfo.size() < num)
         ri[i->first] = i->second.m_sLocInfo.size();
   }

   return ri.size();
}
