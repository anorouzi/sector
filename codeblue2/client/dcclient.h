/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/07/2008
*****************************************************************************/

#ifndef __SPHERE_CLIENT_H__
#define __SPHERE_CLIENT_H__

#include "client.h"
#include <index.h>
#include <pthread.h>
#include <string>
#include <transport.h>

using namespace std;

class SphereStream
{
public:
   SphereStream();
   ~SphereStream();

public:
   int init(const vector<string>& files);

   int init(const int& num);
   void setOutputPath(const string& path, const string& name);

   int setSeg(const int64_t& start, const int64_t& end);

public:
   string m_strPath;
   string m_strName;

   vector<string> m_vFiles;	// list of files
   vector<int64_t> m_vSize;	// size per file
   vector<int64_t> m_vRecNum;	// number of record per file

   vector< set<Address, AddrComp> > m_vLocation;            // locations for each file

   int m_iFileNum;		// number of files
   int64_t m_llSize;		// total data size
   int64_t m_llRecNum;		// total number of records
   int64_t m_llStart;		// start point (record)
   int64_t m_llEnd;		// end point (record), -1 means the last record

   int m_iStatus;		// 0: uninitialized, 1: initialized, -1: bad
};

struct SphereResult
{
   int m_iResID;

   char* m_pcData;
   int m_iDataLen;
   int64_t* m_pllIndex;
   int m_iIndexLen;

   string m_strOrigFile;
   int64_t m_llOrigStartRec;
   int64_t m_llOrigEndRec;

   string m_strIP;
   int m_iPort;
};

class SphereProcess: public Client
{
public:
   SphereProcess();
   ~SphereProcess();

   int loadOperator(const char* library);

   int run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param = NULL, const int& size = 0);

   // rows:
   // 	n (n > 0): n rows per time
   //	0: no rows, one file per time
   //	-1: all rows in each segment

   int read(SphereResult*& res, const bool& inorder = false, const bool& wait = true);
   int checkProgress();
   int close();

private:
   string m_strOperator;
   char* m_pcParam;
   int m_iParamSize;
   SphereStream* m_pInput;
   SphereStream* m_pOutput;
   int m_iRows;
   int m_iOutputType;
   char* m_pOutputLoc;
   int m_iSPENum;

   struct DS
   {
      int m_iID;
      string m_strDataFile;
      int64_t m_llOffset;
      int64_t m_llSize;
      int m_iSPEID;
      int m_iStatus;			// 0: not started yet; 1: in progress; 2: done, result ready; 3: result read
      set<Address, AddrComp>* m_pLoc;	// locations of DS
      SphereResult* m_pResult;
   };
   vector<DS*> m_vpDS;
   pthread_mutex_t m_DSLock;

   struct SPE
   {
      uint32_t m_uiID;
      string m_strIP;
      int m_iPort;
      DS* m_pDS;
      int m_iStatus;			// -1: bad; 0: ready; 1; running
      int m_iProgress;			// 0 - 100 (%)
      timeval m_StartTime;
      timeval m_LastUpdateTime;
      Transport m_DataChn;
      int m_iShufflerPort;              // GMP port for the shuffler on this SPE
   };
   vector<SPE> m_vSPE;

   int m_iProgress;		// progress, 0..100
   int m_iAvgRunTime;		// average running time, in seconds
   int m_iTotalDS;
   int m_iTotalSPE;
   int m_iAvailRes;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;
   int m_iMaxUnitSize;

private:
   int prepareSPE(const char* spenodes);
   int segmentData();
   int prepareOutput();

   static void* run(void*);
   pthread_mutex_t m_RunLock;

   int start();
   int checkSPE();
   int startSPE(SPE& s, DS* d);

   int readResult(SPE* s);
};

#endif
