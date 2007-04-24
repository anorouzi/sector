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

class CIndex
{
public:
   CIndex();
   ~CIndex();

public:
   int lookup(const string& filename, set<CFileAttr, CAttrComp>* attr = NULL);
   int insert(const CFileAttr& attr);
   int remove(const string& filename);
   void updateNameServer(const string& filename, const Node& loc);
   void removeCopy(const CFileAttr& attr);

public:
   int getFileList(map<string, set<CFileAttr, CAttrComp> >& list);

private:
   map<string, set<CFileAttr, CAttrComp> > m_mFileList;

   pthread_mutex_t m_IndexLock;

private:
   int64_t m_llTotalSize;
};

}; // namespace

#endif
