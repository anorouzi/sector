/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 03/06/2009
*****************************************************************************/


#ifndef __CB_DATACHN_H__
#define __CB_DATACHN_H__

#include <transport.h>
#include <topology.h>
#include <map>
#include <string>

class DataChn
{
public:
   DataChn();
   ~DataChn();

   int init(const std::string& ip, int& port);
   int getPort() {return m_iPort;}

public:
   bool isConnected(const std::string& ip, int port);

   int connect(const std::string& ip, int port);
   int remove(const std::string& ip, int port);

   int setCryptoKey(const std::string& ip, int port, unsigned char key[16], unsigned char iv[8]);

   int send(const std::string& ip, int port, int session, const char* data, int size, bool secure = false);
   int recv(const std::string& ip, int port, int session, char*& data, int& size, bool secure = false);
   int64_t sendfile(const std::string& ip, int port, int session, std::ifstream& ifs, int64_t offset, int64_t size, bool secure = false);
   int64_t recvfile(const std::string& ip, int port, int session, std::ofstream& ofs, int64_t offset, int64_t& size, bool secure = false);

   int recv4(const std::string& ip, int port, int session, int32_t& val);
   int recv8(const std::string& ip, int port, int session, int64_t& val);

private:
   struct RcvData
   {
      int m_iSession;
      int m_iSize;
      char* m_pcData;
   };

   struct ChnInfo
   {
      Transport* m_pTrans;
      std::vector<RcvData> m_vDataQueue;
      pthread_mutex_t m_SndLock;
      pthread_mutex_t m_RcvLock;
      pthread_mutex_t m_QueueLock;
   };

   std::map<Address, ChnInfo*, AddrComp> m_mChannel;

   Transport m_Base;
   std::string m_strIP;
   int m_iPort;

   pthread_mutex_t m_ChnLock;

private:
   ChnInfo* locate(const std::string& ip, int port);
};


#endif
