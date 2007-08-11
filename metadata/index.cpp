/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 06/15/2007
*****************************************************************************/


#include <index.h>
#include <util.h>
#include <iostream>
using namespace std;
using namespace cb;


LocalFileIndex::LocalFileIndex()
{
   Sync::initMutex(m_IndexLock);
}

LocalFileIndex::~LocalFileIndex()
{
   Sync::releaseMutex(m_IndexLock);
}

int LocalFileIndex::lookup(const string& filename, string& dir, CFileAttr* attr)
{
   Sync::enterCS(m_IndexLock);

   map<string, CFileAttr>::iterator i = m_mNameIndex.find(filename);

   if (i == m_mNameIndex.end())
   {
      Sync::leaveCS(m_IndexLock);
      return -1;
   }

   dir = m_mDir[filename];

   if (NULL != attr)
      *attr = i->second;

   Sync::leaveCS(m_IndexLock);

   return 1;
}

int LocalFileIndex::insert(const CFileAttr& attr, const string& dir, const Node* n)
{
   Sync::enterCS(m_IndexLock);

   m_mNameIndex[attr.m_pcName] = attr;

   Node tmp;
   Node* node = (Node*)n;
   if (NULL == n)
   {
      strcpy(tmp.m_pcIP, "");
      tmp.m_iAppPort = 0;
      node = &tmp;
   }

   map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.find(*node);
   if (i == m_mLocIndex.end())
   {
      set<string> fl;
      i = m_mLocIndex.insert(m_mLocIndex.begin(), pair<Node, set<string> >(*node, fl));
   }
   i->second.insert(attr.m_pcName);
   m_mLocInfo[attr.m_pcName] = *node;
   m_mDir[attr.m_pcName] = dir;

   Sync::leaveCS(m_IndexLock);

   return 1;  
}

void LocalFileIndex::remove(const string& filename)
{
   Sync::enterCS(m_IndexLock);

   map<string, CFileAttr>::iterator i = m_mNameIndex.find(filename);

   if (i == m_mNameIndex.end())
   {
      Sync::leaveCS(m_IndexLock);
      return;
   }

   m_mNameIndex.erase(i);

   Node& n = m_mLocInfo[filename];
   m_mLocIndex[n].erase(filename);
   if (m_mLocIndex[n].empty())
      m_mLocIndex.erase(n);

   m_mLocInfo.erase(filename);
   m_mDir.erase(filename);

   Sync::leaveCS(m_IndexLock);
}

void LocalFileIndex::updateNameServer(const string& filename, const Node& loc)
{
   Sync::enterCS(m_IndexLock);

   map<string, Node>::iterator i = m_mLocInfo.find(filename);

   if (i == m_mLocInfo.end())
   {
      Sync::leaveCS(m_IndexLock);
      return;
   }

   Node& tmp = m_mLocInfo[filename];
   m_mLocIndex[tmp].erase(filename);
   if (m_mLocIndex[tmp].empty())
      m_mLocIndex.erase(tmp);

   m_mLocInfo[filename] = loc;
   m_mLocIndex[loc].insert(filename);

   Sync::leaveCS(m_IndexLock);
}

int LocalFileIndex::getLocIndex(map<Node, set<string>, NodeComp>& li)
{
   Sync::enterCS(m_IndexLock);

   li.clear();
   for (map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.begin(); i != m_mLocIndex.end(); ++ i)
   {
      set<string> tmp;
      map<Node, set<string>, NodeComp>::iterator j = li.insert(li.end(), pair<Node, set<string> >(i->first, tmp));
      for (set<string>::iterator k = i->second.begin(); k != i->second.end(); ++ k)
         j->second.insert(*k);
   }

   Sync::leaveCS(m_IndexLock);

   return li.size();
}

int LocalFileIndex::getFileList(set<string>& fl)
{
   Sync::enterCS(m_IndexLock);

   fl.clear();
   for (map<string, CFileAttr>::iterator i = m_mNameIndex.begin(); i != m_mNameIndex.end(); ++ i)
      fl.insert(fl.end(), i->first);

   Sync::leaveCS(m_IndexLock);

   return fl.size();
}

RemoteFileIndex::RemoteFileIndex()
{
   Sync::initMutex(m_IndexLock);
}

RemoteFileIndex::~RemoteFileIndex()
{
   Sync::releaseMutex(m_IndexLock);
}

int RemoteFileIndex::lookup(const string& filename, CFileAttr* attr, set<Node, NodeComp>* nl)
{
   Sync::enterCS(m_IndexLock);

   map<string, CFileAttr>::iterator i = m_mNameIndex.find(filename);

   if (i == m_mNameIndex.end())
   {
      Sync::leaveCS(m_IndexLock);
      return -1;
   }

   if (NULL != attr)
      *attr = i->second;

   if (NULL != nl)
   {
      nl->clear();
      for (set<Node, NodeComp>::iterator j = m_mLocInfo[filename].begin(); j != m_mLocInfo[filename].end(); ++ j)
         nl->insert(*j);
   }

   Sync::leaveCS(m_IndexLock);

   return m_mLocInfo[filename].size();
}

int RemoteFileIndex::insert(const CFileAttr& attr, const Node& n)
{
   Sync::enterCS(m_IndexLock);

   map<string, CFileAttr>::iterator i = m_mNameIndex.find(attr.m_pcName);

   if (i == m_mNameIndex.end())
      m_mNameIndex[attr.m_pcName] = attr;

   m_mLocInfo[attr.m_pcName].insert(n);
   m_mLocIndex[n].insert(attr.m_pcName);

   Sync::leaveCS(m_IndexLock);

   return 1;
}

void RemoteFileIndex::remove(const string& filename)
{
   Sync::enterCS(m_IndexLock);

   map<string, CFileAttr>::iterator i = m_mNameIndex.find(filename);

   if (i != m_mNameIndex.end())
      m_mNameIndex.erase(i);

   for (set<Node, NodeComp>::iterator j = m_mLocInfo[filename].begin(); j != m_mLocInfo[filename].end(); ++ j)
   {
      m_mLocIndex[*j].erase(filename);
      if (m_mLocIndex[*j].empty())
         m_mLocIndex.erase(*j);
   }

   m_mLocInfo.erase(filename);

   Sync::leaveCS(m_IndexLock);
}

void RemoteFileIndex::remove(const Node& n)
{
   cout << "remove IP index " << n.m_pcIP << endl;

   Sync::enterCS(m_IndexLock);

   map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.find(n);

   if (i == m_mLocIndex.end())
   {
      Sync::leaveCS(m_IndexLock);
      return;
   }

   for (set<string>::iterator f = i->second.begin(); f != i->second.end(); ++ f)
   {
      m_mLocInfo[*f].erase(n);
      if (m_mLocInfo[*f].empty())
      {
         m_mLocInfo.erase(*f);
         m_mNameIndex.erase(*f);
      }
   }

   m_mLocIndex.erase(i);

   Sync::leaveCS(m_IndexLock);
}

void RemoteFileIndex::removeCopy(const string& filename, const Node& n)
{
   Sync::enterCS(m_IndexLock);

   m_mLocIndex[n].erase(filename);
   if (m_mLocIndex[n].empty())
      m_mLocIndex.erase(n);

   m_mLocInfo[filename].erase(n);
   if (m_mLocInfo[filename].empty())
      m_mLocInfo.erase(filename);

   if (m_mLocInfo.find(filename) == m_mLocInfo.end())
      m_mNameIndex.erase(filename);

   Sync::leaveCS(m_IndexLock);
}

int RemoteFileIndex::getLocIndex(map<Node, set<string>, NodeComp>& li)
{
   Sync::enterCS(m_IndexLock);

   li.clear();
   for (map<Node, set<string>, NodeComp>::iterator i = m_mLocIndex.begin(); i != m_mLocIndex.end(); ++ i)
   {
      set<string> tmp;
      map<Node, set<string>, NodeComp>::iterator j = li.insert(li.end(), pair<Node, set<string> >(i->first, tmp));
      for (set<string>::iterator k = i->second.begin(); k != i->second.end(); ++ k)
         j->second.insert(*k);
   }

   Sync::leaveCS(m_IndexLock);

   return li.size();
}
