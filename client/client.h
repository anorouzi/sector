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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/07/2007
*****************************************************************************/


#ifndef __CB_CLIENT_H__
#define __CB_CLIENT_H__

#include <gmp.h>
#include <node.h>
#include <file.h>
#include <data.h>

namespace cb
{

class Client
{
friend class File;
friend class Query;
friend class Process;

private:
   Client();
   ~Client();

public:
   static int init(const string& server, const int& port);
   static int close();
   static int lookup(const string& name, vector<Node>& nl);
   static int checkServStatus(const vector<Node>& nl, vector<NodeInfo>& il);

   static File* createFileHandle();
   static void releaseFileHandle(File* f);
   static int stat(const string& filename, CFileAttr& attr);

   static Query* createQueryHandle();
   static void releaseQueryHandle(Query* q);
   static int getSemantics(const string& name, vector<DataAttr>& attr);

   static Process* createJob();
   static int releaseJob(Process* proc);

protected:
   static int lookup(const string& name, Node* n);

protected:
   static string m_strServerHost;
   static int m_iServerPort;
   static CGMP* m_pGMP;

private:
   static int m_iCount;
};

typedef Client Sector;

}; // namespace

#endif
