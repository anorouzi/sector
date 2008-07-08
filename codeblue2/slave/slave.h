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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/07/2008
*****************************************************************************/


#ifndef __SERVER_H__
#define __SERVER_H__

#include <gmp.h>
#include <transport.h>
#include <conf.h>
#include <index.h>

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
      int mode;			// file access mode
      int transid;		// transaction ID
      string client_ip;		// client IP
      int client_data_port;	// client data channel port
   };

   struct Param3
   {
      Slave* serv_instance;
      string filename;
      time_t timestamp;
   };

   struct Param4
   {
      Slave* serv_instance;	// self
      Transport* datachn;	// data channel
      string client_ip;		// client IP
      int client_ctrl_port;	// client GMP port
      int client_data_port;	// client data port
      int key;			// client key
      int speid;		// speid
      string function;		// SPE operator
      int rows;                 // number of rows per processing: -1 means all in the block
      char* param;		// SPE parameter
      int psize;		// parameter size
   };

   struct Param5
   {
      Slave* serv_instance;     // self
      string client_ip;         // client IP
      int client_ctrl_port;     // client GMP port
      string path;		// output path
      string filename;		// SPE output file name
      int dsnum;		// number of data segments (results to expect)
      CGMP* gmp;		// GMP
   };

   static void* fileHandler(void* p2);
   static void* copy(void* p3);
   static void* SPEHandler(void* p4);
   static void* SPEShuffler(void* p5);

private:
   int SPEReadData(const string& datafile, const int64_t& offset, int& size, int64_t* index, const int64_t& totalrows, char*& block);
   int SPESendResult(const int& speid, const int& buckets, const SPEResult& result, const string& localfile, Transport* datachn, char* locations, map<Address, Transport*, AddrComp>* outputchn);

private:
   int createDir(const string& path);
   int createSysDir();

private:
   int report(const int32_t& transid, const string& path, const int& change = 0);

private:
   CGMP m_GMP;

   string m_strLocalHost;
   int m_iLocalPort;

   string m_strMasterHost;
   string m_strMasterIP;
   int m_iMasterPort;

   string m_strHomeDir;
   time_t m_HomeDirMTime;

   SlaveConf m_SysConfig;

   Index m_LocalFile;

   string m_strBase;
};

#endif
