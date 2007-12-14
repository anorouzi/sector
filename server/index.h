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


#ifndef __INDEX_H__
#define __INDEX_H__

#include <file.h>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <node.h>

using namespace std;

namespace cb
{

class LocalFileIndex
{
public:
   LocalFileIndex();
   ~LocalFileIndex();

public:
   int lookup(const string& filename, string& dir, CFileAttr* attr = NULL);
   int insert(const CFileAttr& attr, const string& dir, const Node* n = NULL);
   void remove(const string& filename);
   void updateNameServer(const string& filename, const Node& loc);
   int getLocIndex(vector<Node>& li);
   int getFileList(set<string>& fl);
   int getFileList(vector<string>& fl, const Node& n);
   int updateFileLock(const string& filename, const int& iotype) {return 0;}

private:
   struct LocalIndexInfo
   {
      CFileAttr m_Attr;		// file attribute
      Node m_Loc;		// meta server location
      string m_strDir;		// local directory
   };

   map<string, LocalIndexInfo> m_mFileIndex;		// name index
   map<Node, set<string>, NodeComp> m_mLocIndex;	// remote metadata location ordered by node

private:
   pthread_mutex_t m_IndexLock;
};

class RemoteFileIndex
{
public:
   RemoteFileIndex();
   ~RemoteFileIndex();

public:
   int lookup(const string& filename, CFileAttr* attr = NULL, set<Node, NodeComp>* nl = NULL);
   bool check(const string& filename, const Node& n);
   int insert(const CFileAttr& attr, const Node& n);
   void remove(const string& filename);
   void remove(const Node& n);
   void removeCopy(const string& filename, const Node& n);
   int getFileList(vector<string>& fl, const Node& n);
   int getReplicaInfo(map<string, int>& ri, const unsigned int& num = 1);

private:
   struct RemoteIndexInfo
   {
      CFileAttr m_Attr;			// file attribute
      set<Node, NodeComp> m_sLocInfo;	// file locations 
   };

   struct RemoteNodeInfo
   {
      set<string> m_sFileList;		// list of files on remote node
      uint64_t m_ullTimeStamp;		// time stamp of last probe message
   };

   map<string, RemoteIndexInfo> m_mFileIndex;
   map<Node, RemoteNodeInfo, NodeComp> m_mLocIndex;

private:
   pthread_mutex_t m_IndexLock;
};

}; // namespace

#endif
