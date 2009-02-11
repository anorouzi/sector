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
   Yunhong Gu [gu@lac.uic.edu], last updated 11/09/2008
*****************************************************************************/


#ifndef __SERVER_H__
#define __SERVER_H__

#include <gmp.h>
#include <transport.h>
#include <conf.h>
#include <index.h>
#include <log.h>
#include <sphere.h>


typedef int (*SPHERE_PROCESS)(const SInput*, SOutput*, SFile*);
typedef int (*MR_MAP)(const SInput*, SOutput*, SFile*);
typedef int (*MR_PARTITION)(const char*, int, void*, int);
typedef int (*MR_COMPARE)(const char*, int, const char*, int);
typedef int (*MR_REDUCE)(const SInput*, SOutput*, SFile*);


class SPEResult
{
public:
   ~SPEResult();

public:
   void init(const int& n);
   void addData(const int& bucketid, const char* data, const int64_t& len);
   void clear();

public:
   int m_iBucketNum;

   vector<int32_t> m_vIndexLen;
   vector<int64_t*> m_vIndex;
   vector<int32_t> m_vIndexPhyLen;
   vector<int32_t> m_vDataLen;
   vector<char*> m_vData;
   vector<int32_t> m_vDataPhyLen;

   int64_t m_llTotalDataSize;
};

class SPEDestination
{
public:
   SPEDestination();
   ~SPEDestination();

public:
   void init(const int& buckets);

public:
   int* m_piSArray;
   int* m_piRArray;
   string m_strLocalFile;
   char m_pcLocalFileID[64];
   char* m_pcOutputLoc;
   map<Address, Transport*, AddrComp> m_mOutputChn;
};

struct MRRecord
{
   char* m_pcData;
   int m_iSize;

   MR_COMPARE m_pCompRoutine;
};

struct ltrec
{
   bool operator()(const MRRecord& r1, const MRRecord& r2) const
   {
      return (r1.m_pCompRoutine(r1.m_pcData, r1.m_iSize, r2.m_pcData, r2.m_iSize) < 0);
   }
};

class SlaveStat
{
public:
   int64_t m_llStartTime;
   int64_t m_llTimeStamp;

   int64_t m_llDataSize;
   int64_t m_llAvailSize;
   int64_t m_llCurrMemUsed;
   int64_t m_llCurrCPUUsed;

   int64_t m_llTotalInputData;
   int64_t m_llTotalOutputData;

   map<string, int64_t> m_mSysIndInput;
   map<string, int64_t> m_mSysIndOutput;
   map<string, int64_t> m_mCliIndInput;
   map<string, int64_t> m_mCliIndOutput;

public:
   void init();
   void refresh();

   void updateIO(const string& ip, const int64_t& size, const int& type);
   int serializeIOStat(char* buf, unsigned int size);

private:
   pthread_mutex_t m_StatLock;   
};

class Slave
{
public:
   Slave();
   ~Slave();

public:
   int init(const char* base = NULL);
   int connect();
   void run();

private:
   struct Param2
   {
      Slave* serv_instance;	// self
      string filename;		// filename
      Transport* datachn;	// data channel
      int dataport;		// data port
      int mode;			// file access mode
      int transid;		// transaction ID
      int key;                  // client key

      string src_ip;		// downlink IP
      int src_port;		// downlink port
      string dst_ip;		// uplink IP
      int dst_port;		// uplink port

      unsigned char crypto_key[16];
      unsigned char crypto_iv[8];
   };

   struct Param3
   {
      Slave* serv_instance;
      string src;
      string dst;
      time_t timestamp;
   };

   struct Param4
   {
      Slave* serv_instance;	// self
      Transport* datachn;	// data channel
      int transid;		// transaction id
      string client_ip;		// client IP
      int client_ctrl_port;	// client GMP port
      int client_data_port;	// client data port
      int key;			// client key
      int speid;		// speid
      string function;		// SPE or Map operator
      int rows;                 // number of rows per processing: -1 means all in the block
      char* param;		// SPE parameter
      int psize;		// parameter size
      int type;			// process type
   };

   struct Param5
   {
      Slave* serv_instance;     // self
      int transid;		// transaction id
      string client_ip;         // client IP
      int client_ctrl_port;     // client GMP port
      string path;		// output path
      string filename;		// SPE output file name
      int bucketnum;		// number of buckets
      CGMP* gmp;		// GMP
      int key;			// client key
      int bucketid;		// bucket id
      int type;			// process type
      string function;          // Reduce operator
      char* param;              // Reduce parameter
      int psize;                // parameter size
   };

   static void* fileHandler(void* p2);
   static void* copy(void* p3);
   static void* SPEHandler(void* p4);
   static void* SPEShuffler(void* p5);

private:
   int SPEReadData(const string& datafile, const int64_t& offset, int& size, int64_t* index, const int64_t& totalrows, char*& block);
   int sendResultToFile(const SPEResult& result, const string& localfile, const int64_t& offset);
   int sendResultToBuckets(const int& speid, const int& buckets, const SPEResult& result, char* locations, map<Address, Transport*, AddrComp>* outputchn);
   int sendResultToClient(const int& buckets, const int* sarray, const int* rarray, const SPEResult& result, Transport* datachn);

   int acceptLibrary(const int& key, Transport* datachn);
   int openLibrary(const int& key, const string& lib, void*& lh);
   int getSphereFunc(void* lh, const string& function, SPHERE_PROCESS& process);
   int getMapFunc(void* lh, const string& function, MR_MAP& map, MR_PARTITION& partition);
   int getReduceFunc(void* lh, const string& function, MR_COMPARE& compare, MR_REDUCE& reduce);
   int closeLibrary(void* lh);

   int sort(const string& bucket, MR_COMPARE comp, MR_REDUCE red);
   int reduce(vector<MRRecord>& vr, const string& bucket, MR_REDUCE red, void* param, int psize);

   int processData(SInput& input, SOutput& output, SFile& file, SPEResult& result, SPHERE_PROCESS process, MR_MAP map, MR_PARTITION partition);
   int deliverResult(const int& buckets, const int& speid, SPEResult& result, SPEDestination& dest);

private:
   int createDir(const string& path);
   int createSysDir();
   string reviseSysCmdPath(const string& path);
   int move(const string& src, const string& dst);

private:
   int report(const int32_t& transid, const string& path, const int& change = 0);
   int reportSphere(const int32_t& transid, const std::vector<Address>* bad = NULL);

   void logError(int type, const string& ip, const int& port, const string& name);

   int checkBadDest(std::multimap<int64_t, Address>& sndspd, std::vector<Address>& bad);

private:
   int m_iSlaveID;			// unique ID assigned by the master

   CGMP m_GMP;				// GMP messenger

   string m_strLocalHost;		// local host IP address
   int m_iLocalPort;			// local port number

   string m_strMasterHost;		// host name of the master node
   string m_strMasterIP;		// IP address of the master node
   int m_iMasterPort;			// port number of the master node

   string m_strHomeDir;			// data directory
   time_t m_HomeDirMTime;		// last modified time

   string m_strBase;                    // the local directory that stores Sector configuration files
   SlaveConf m_SysConfig;		// system configuration
   Index m_LocalFile;			// local file index
   SlaveStat m_SlaveStat;		// slave statistics
   SectorLog m_SectorLog;		// slave log
};

#endif
