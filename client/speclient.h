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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/25/2007
*****************************************************************************/

#ifndef __SECTOR_H__
#define __SECTOR_H__

#include "client.h"
#include <string>
#include <pthread.h>
#include <transport.h>

using namespace std;

namespace cb
{

class Process;

class Stream
{
public:
   Stream();
   ~Stream();

public:
   int init(const vector<string>& files);
   int init(const int& num);
   void setName(const string& name);
   int setSeg(const int64_t& start, const int64_t& end);
   int getSeg(int64_t& start, int64_t& end);
   int getSize(int64_t& size);

public:
   string m_strName;

   vector<string> m_vFiles;	// list of files
   vector<int64_t> m_vSize;	// size per file
   vector<int64_t> m_vRecNum;	// number of record per file

   vector< set<Node, NodeComp> > m_vLocation;            // locations for each bucket

   int m_iFileNum;		// number of files
   int64_t m_llSize;		// total data size
   int64_t m_llRecNum;		// total number of records
   int64_t m_llStart;		// start point (record)
   int64_t m_llEnd;		// end point (record), -1 means the last record

   int m_iStatus;		// 0: uninitialized, 1: initialized, -1: bad
};

struct Result
{
   int m_iResID;

   char* m_pcData;
   int m_iDataLen;
   int64_t* m_pllIndex;
   int m_iIndexLen;

   string m_strOrigFile;
   int64_t m_llOrigStartRec;
   int64_t m_llOrigEndRec;
};

class Process
{
friend class Client;

public:
   Process();
   ~Process();

   int run(const Stream& input, Stream& output, string op, const int& rows, const char* param = NULL, const int& size = 0);
   int read(Result*& res, const bool& inorder = true, const bool& wait = true);
   int checkProgress();
   int close();

private:
   string m_strOperator;
   char* m_pcParam;
   int m_iParamSize;
   Stream* m_pOutput;
   int m_iOutputType;

   struct DS
   {
      int m_iID;

      string m_strDataFile;
      int64_t m_llOffset;
      int64_t m_llSize;

      int m_iSPEID;

      int m_iStatus;		// 0: not started yet; 1: in progress; 2: done, result ready; 3: result read

      Result* m_pResult;
   };
   vector<DS> m_vDS;

   struct SPE
   {
      uint32_t m_uiID;
      string m_strIP;
      int m_iPort;

      DS* m_pDS;
      int m_iStatus;		// -1: bad; 0: ready; 1; running
      int m_iProgress;

      timeval m_StartTime;
      timeval m_LastUpdateTime;

      Transport m_DataChn;
   };
   vector<SPE> m_vSPE;

   int m_iProgress;
   int m_iAvgRunTime;
   int m_iTotalDS;
   int m_iTotalSPE;
   int m_iAvailRes;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;
   int m_iMaxUnitSize;

   CGMP m_GMP;

private:
   int prepareSPE();
   static void* run(void*);
   int checkSPE();
   int startSPE(SPE& s);
};

}; // namespace cb

#endif
