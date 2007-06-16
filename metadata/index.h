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


#ifndef __INDEX_H__
#define __INDEX_H__

#include <file.h>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <util.h>
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
   int lookup(const string& filename, CFileAttr* attr = NULL);
   int insert(const CFileAttr& attr, const Node* n = NULL);
   void remove(const string& filename);
   void updateNameServer(const string& filename, const Node& loc);
   int getLocIndex(map<Node, set<string>, NodeComp>& li);
   int getFileList(set<string>& fl);

private:
   map<string, CFileAttr> m_mNameIndex;
   map<string, Node> m_mLocInfo;
   map<Node, set<string>, NodeComp> m_mLocIndex;

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
   int insert(const CFileAttr& attr, const Node& n);
   void remove(const string& filename);
   void remove(const Node& n);
   void removeCopy(const string& filename, const Node& n);
   int getLocIndex(map<Node, set<string>, NodeComp>& li);

private:
   map<string, CFileAttr> m_mNameIndex;
   map<string, set<Node, NodeComp> > m_mLocInfo;
   map<Node, set<string>, NodeComp> m_mLocIndex;

private:
   pthread_mutex_t m_IndexLock;
};

}; // namespace

#endif
