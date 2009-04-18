/*****************************************************************************
Copyright � 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 01/26/2009
*****************************************************************************/

#ifndef __SPHERE_CLIENT_H__
#define __SPHERE_CLIENT_H__

#include "client.h"
#include <index.h>
#include <pthread.h>
#include <string>

class SphereStream
{
public:
   SphereStream();
   ~SphereStream();

public:
   int init(const vector<string>& files);

   int init(const int& num);
   void setOutputPath(const string& path, const string& name);

public:
   string m_strPath;
   string m_strName;

   std::vector<std::string> m_vFiles;	// list of files
   std::vector<int64_t> m_vSize;	// size per file
   std::vector<int64_t> m_vRecNum;	// number of record per file

   std::vector< set<Address, AddrComp> > m_vLocation;            // locations for each file

   int m_iFileNum;		// number of files
   int64_t m_llSize;		// total data size
   int64_t m_llRecNum;		// total number of records
   int64_t m_llStart;		// start point (record)
   int64_t m_llEnd;		// end point (record), -1 means the last record

   int m_iStatus;		// 0: uninitialized, 1: initialized, -1: bad
};

class SphereResult
{
public:
   SphereResult();
   ~SphereResult();

public:
   int m_iResID;		// result ID

   char* m_pcData;		// result data
   int m_iDataLen;		// result data length
   int64_t* m_pllIndex;		// result data index
   int m_iIndexLen;		// result data index length

   std::string m_strOrigFile;	// original input file
   int64_t m_llOrigStartRec;	// first record of the original input file
   int64_t m_llOrigEndRec;	// last record of the original input file

   std::string m_strIP;
   int m_iPort;
};

class SphereProcess: public Client
{
public:
   SphereProcess();
   ~SphereProcess();

   int close();

   int loadOperator(const char* library);

   int run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param = NULL, const int& size = 0, const int& type = 0);
   int run_mr(const SphereStream& input, SphereStream& output, const string& mr, const int& rows, const char* param = NULL, const int& size = 0);

   // rows:
   // 	n (n > 0): n rows per time
   //	0: no rows, one file per time
   //	-1: all rows in each segment

   int read(SphereResult*& res, const bool& inorder = false, const bool& wait = true);
   int checkProgress();
   int checkMapProgress();
   int checkReduceProgress();

   inline void setMinUnitSize(int size) {m_iMinUnitSize = size;}
   inline void setMaxUnitSize(int size) {m_iMaxUnitSize = size;}
   inline void setProcNumPerNode(int num) {m_iCore = num;}
   inline void setDataMoveAttr(bool move) {m_bDataMove = move;}

private:
   int m_iProcType;			// 0: sphere 1: mapreduce

   std::string m_strOperator;
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
      int m_iID;				// DS ID
      string m_strDataFile;			// input data file
      int64_t m_llOffset;			// input data offset
      int64_t m_llSize;				// input data size
      int m_iSPEID;				// processing SPE
      std::set<Address, AddrComp>* m_pLoc;	// locations of DS

      int m_iStatus;                            // 0: not started yet; 1: in progress; 2: done, result ready; 3: result read; -1: failed
      int m_iRetryNum;				// number of retry due to failure
      SphereResult* m_pResult;			// processing result
   };
   std::map<int, DS*> m_mpDS;
   pthread_mutex_t m_DSLock;

   struct SPE
   {
      int32_t m_iID;
      std::string m_strIP;		// SPE IP
      int m_iPort;			// SPE GMP port
      int m_iDataPort;			// SPE data port
      DS* m_pDS;
      int m_iStatus;			// -1: abandond; 0: uninitialized; 1: ready; 2; running
      int m_iProgress;			// 0 - 100 (%)
      timeval m_StartTime;
      timeval m_LastUpdateTime;
      int m_iSession;			// SPE session ID
   };
   std::map<int, SPE> m_mSPE;

   struct BUCKET
   {
      int32_t m_iID;
      std::string m_strIP;		// slave IP address
      int m_iPort;			// slave GMP port
      int m_iDataPort;			// slave Data port
      int m_iShufflerPort;		// Shuffer GMP port
      int m_iSession;			// Shuffler session ID
      int m_iProgress;
      timeval m_LastUpdateTime;
   };
   std::map<int, BUCKET> m_mBucket;

   int m_iProgress;		// progress, 0..100
   int m_iAvgRunTime;		// average running time, in seconds
   int m_iTotalDS;		// total number of data segments
   int m_iTotalSPE;		// total number of SPEs
   int m_iAvailRes;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;		// minimum data segment size
   int m_iMaxUnitSize;		// maximum data segment size, must be smaller than physical memory
   int m_iCore;			// number of processing instances on each node
   bool m_bDataMove;		// if source data is allowed to move for Sphere process

   struct OP
   {
      std::string m_strLibrary;
      std::string m_strLibPath;
      int m_iSize;
   };
   std::vector<OP> m_vOP;
   int loadOperator(SPE& s);

private:
   int prepareSPE(const char* spenodes);
   int segmentData();
   int prepareOutput(const char* spenodes);

   static void* run(void*);
   pthread_mutex_t m_RunLock;

   int start();
   int checkSPE();
   int startSPE(SPE& s, DS* d);
   int connectSPE(SPE& s);
   int checkBucket();
   int readResult(SPE* s);
};

#endif
