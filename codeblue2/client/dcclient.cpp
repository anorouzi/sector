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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/31/2008
*****************************************************************************/

#include "dcclient.h"
#include <constant.h>
#include <errno.h>
#include <iostream>

using namespace std;

SphereStream::SphereStream():
m_iFileNum(0),
m_llSize(0),
m_llRecNum(0),
m_llStart(0),
m_llEnd(-1),
m_iStatus(0)
{
   m_vFiles.clear();
   m_vRecNum.clear();
   m_vSize.clear();
   m_vLocation.clear();
}

SphereStream::~SphereStream()
{

}

int Client::dataInfo(const vector<string>& files, vector<string>& info)
{
   SectorMsg msg;
   msg.setType(201);
   msg.setKey(g_iKey);

   int offset = 0;
   int32_t size = -1;
   for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++ i)
   {
      string path = revisePath(*i);
      size = path.length() + 1;
      msg.setData(offset, (char*)&size, 4);
      msg.setData(offset + 4, path.c_str(), size);
      offset += 4 + size;
   }

   size = -1;
   msg.setData(offset, (char*)&size, 4);

   if (g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   char* buf = msg.getData();
   size = msg.m_iDataLength - SectorMsg::m_iHdrSize;

   while (size > 0)
   {
      info.insert(info.end(), buf);
      size -= strlen(buf) + 1;
      buf += strlen(buf) + 1;
   }

   return info.size();
}

int SphereStream::init(const vector<string>& files)
{
   vector<string> datainfo;
   int res = Client::dataInfo(files, datainfo);
   if (res < 0)
      return res;

   m_iFileNum = datainfo.size();
   if (0 == m_iFileNum)
      return 0;

   m_iStatus = -1;

   m_vFiles.resize(m_iFileNum);
   m_vSize.resize(m_iFileNum);
   m_vRecNum.resize(m_iFileNum);
   m_vLocation.resize(m_iFileNum);
   vector<string>::iterator f = m_vFiles.begin();
   vector<int64_t>::iterator s = m_vSize.begin();
   vector<int64_t>::iterator r = m_vRecNum.begin();
   vector< set<Address, AddrComp> >::iterator a = m_vLocation.begin();

   bool indexfound = true;

   for (vector<string>::iterator i = datainfo.begin(); i != datainfo.end(); ++ i)
   {
      char* buf = new char[i->length() + 2];
      strcpy(buf, i->c_str());
      buf[strlen(buf) + 1] = '\0';

      //file_name 5105847 -1 192.168.136.30 37209 192.168.136.32 39805

      int n = strlen(buf) + 1;
      char* p = buf;
      for (int j = 0; j < n; ++ j, ++ p)
      {
         if (*p == ' ')
            *p = '\0';
      }
      p = buf;

      *f = p;
      p = p + strlen(p) + 1;
      *s = atoll(p);
      m_llSize += *s;
      p = p + strlen(p) + 1;
      *r = atoi(p);
      p = p + strlen(p) + 1;

      if (*r == -1)
      {
         // no record index found
         m_llRecNum = -1;
         indexfound = false;
      }
      else if (indexfound)
      {
         m_llRecNum += *r;
      }

      // retrieve all the locations
      while (true)
      {
         if (strlen(p) == 0)
            break;

         p ++;

         Address addr;
         addr.m_strIP = p;
         p = p + strlen(p) + 1;
         addr.m_iPort = atoi(p);
         p = p + strlen(p);

         a->insert(addr);
      }

      delete [] buf;

      f ++;
      s ++;
      r ++;
      a ++;
   }

   m_llEnd = m_llRecNum;

   m_iStatus = 1;
   return m_iFileNum;
}

int SphereStream::init(const int& num)
{
   m_iFileNum = num;
   m_llSize = 0;
   m_llRecNum = 0;
   m_llStart = 0;
   m_llEnd = -1;
   m_iStatus = 1;

   if (num <= 0)
      return 0;

   m_vLocation.resize(num);
   m_vFiles.resize(num);
   m_vSize.resize(num);
   m_vRecNum.resize(num);

   for (vector<string>::iterator i = m_vFiles.begin(); i != m_vFiles.end(); ++ i)
      *i = "";
   for (vector<int64_t>::iterator i = m_vSize.begin(); i != m_vSize.end(); ++ i)
      *i = 0;
   for (vector<int64_t>::iterator i = m_vRecNum.begin(); i != m_vRecNum.end(); ++ i)
      *i = 0;

   return num;
}

void SphereStream::setOutputPath(const string& path, const string& name)
{
   m_strPath = path;
   m_strName = name;
}


//
SphereResult::SphereResult():
m_iResID(-1),
m_pcData(NULL),
m_iDataLen(0),
m_pllIndex(NULL),
m_iIndexLen(0)
{
}

SphereResult::~SphereResult()
{
   delete [] m_pcData;
   delete [] m_pllIndex;
}

//
SphereProcess::SphereProcess():
m_iMinUnitSize(1000000),
m_iMaxUnitSize(256000000),
m_iCore(1),
m_bDataMove(true)
{
   m_strOperator = "";
   m_pcParam = NULL;
   m_iParamSize = 0;
   m_pOutput = NULL;
   m_iOutputType = 0;
   m_pOutputLoc = NULL;

   m_vpDS.clear();
   m_vSPE.clear();

   pthread_mutex_init(&m_DSLock, NULL);
   pthread_mutex_init(&m_ResLock, NULL);
   pthread_cond_init(&m_ResCond, NULL);
   pthread_mutex_init(&m_RunLock, NULL);
}

SphereProcess::~SphereProcess()
{
   delete [] m_pcParam;
   delete [] m_pOutputLoc;

   pthread_mutex_destroy(&m_DSLock);
   pthread_mutex_destroy(&m_ResLock);
   pthread_cond_destroy(&m_ResCond);
   pthread_mutex_destroy(&m_RunLock);
}

int SphereProcess::loadOperator(const char* library)
{
   struct stat st;
   if (::stat(library, &st) < 0)
   {
      cerr << "loadOperator: no library found.\n";
      return -1;
   }

   ifstream lib;
   lib.open(library, ios::binary);
   if (lib.bad() || lib.fail())
   {
      cerr << "loadOperator: bad file.\n";
      return -1;
   }
   lib.close();

   // TODO : check ".so"

   vector<string> dir;
   Index::parsePath(library, dir);

   OP op;
   op.m_strLibrary = dir[dir.size() - 1];
   op.m_strLibPath = library;
   op.m_iSize = st.st_size;

   m_vOP.insert(m_vOP.end(), op);

   return 0;
}

int SphereProcess::loadOperator(SPE& s)
{
   for (vector<OP>::iterator i = m_vOP.begin(); i != m_vOP.end(); ++ i)
   {
      int32_t size = i->m_strLibrary.length() + 1;
      s.m_DataChn.send((char*)&size, 4);
      s.m_DataChn.send(i->m_strLibrary.c_str(), size);

      ifstream lib;
      lib.open(i->m_strLibPath.c_str(), ios::binary);
      char* buf = new char[i->m_iSize];
      lib.read(buf, i->m_iSize);
      lib.close();

      s.m_DataChn.send((char*)&(i->m_iSize), 4);
      s.m_DataChn.send(buf, i->m_iSize);
   }

   int32_t size = -1;
   s.m_DataChn.send((char*)&size, 4);

   return 0;
}

int SphereProcess::run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param, const int& size, const int& type)
{
   if (input.m_llSize <= 0)
      return 0;

   pthread_mutex_lock(&m_RunLock);
   pthread_mutex_unlock(&m_RunLock);

   m_iProcType = type;
   m_strOperator = op;
   m_pcParam = new char[size];
   memcpy(m_pcParam, param, size);
   m_iParamSize = size;
   m_pInput = (SphereStream*)&input;
   m_pOutput = &output;
   m_iRows = rows;
   m_iOutputType = m_pOutput->m_iFileNum;

   m_vpDS.clear();
   m_vSPE.clear();

   cout << "JOB " << input.m_llSize << " " << input.m_llRecNum << endl;

   SectorMsg msg;
   msg.setType(202); // locate available SPE
   msg.setKey(g_iKey);
   msg.m_iDataLength = SectorMsg::m_iHdrSize;

   if ((g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0) || (msg.getType() < 0))
   {
      cerr << "unable to locate any SPE.\n";
      return -1;
   }

   m_iSPENum = (msg.m_iDataLength - 4) / 68;
   if (0 == m_iSPENum)
   {
      cerr << "no available SPE found.\n";
      return -1;
   }

   prepareSPE(msg.getData());

   if (segmentData() <= 0)
   {
      cerr << "data segmentation error.\n";
      return -1;
   }

   if (m_iOutputType == -1)
      m_pOutput->init(m_vpDS.size());

   prepareOutput();

   m_iProgress = 0;
   m_iAvgRunTime = 3600;
   m_iTotalDS = m_vpDS.size();
   m_iTotalSPE = m_vSPE.size();
   m_iAvailRes = 0;

   cout << m_vSPE.size() << " spes found! " << m_vpDS.size() << " data seg total." << endl;

   // starting...
   pthread_t reduce;
   pthread_create(&reduce, NULL, run, this);
   pthread_detach(reduce);

   return 0;
}

int SphereProcess::run_mr(const SphereStream& input, SphereStream& output, const string& mr, const int& rows, const char* param, const int& size)
{
   return run(input, output, mr, rows, param, size, 1);
}

int SphereProcess::close()
{
   pthread_mutex_lock(&m_RunLock);
   pthread_mutex_unlock(&m_RunLock);

   m_vSPE.clear();
   m_vpDS.clear();

   return 0;
}

void* SphereProcess::run(void* param)
{
   SphereProcess* self = (SphereProcess*)param;

   pthread_mutex_lock(&self->m_RunLock);

   // start initial round
   self->start();

   bool mapping = true;
   while ((self->m_iProgress < self->m_iTotalDS) || (self->checkBucket() > 0))
   {
      if (0 == self->checkSPE())
         break;

      if (mapping && (self->m_iProgress == self->m_iTotalDS))
      {
         // disconnect all SPEs and close all Shufflers
         for (vector<SPE>::iterator i = self->m_vSPE.begin(); i != self->m_vSPE.end(); ++ i)
         {
            if (i->m_iStatus >= 0)
            i->m_DataChn.close();

            if (i->m_iShufflerPort > 0)
            {
               SectorMsg msg;
               int32_t cmd = -1;
               msg.setData(0, (char*)&cmd, 4);
               int id = 0;
               self->g_GMP.sendto(i->m_strIP.c_str(), i->m_iShufflerPort, id, &msg);
            }
         }

         mapping = false;
      }

      char ip[64];
      int port;
      int tmp;
      SectorMsg msg;
      if (self->g_GMP.recvfrom(ip, port, tmp, &msg, false) < 0)
         continue;

      int32_t id = *(uint32_t*)(msg.getData());

      if (id >= 0)
      {
         uint32_t speid = id;
         vector<SPE>::iterator s = self->m_vSPE.begin();
         for (; s != self->m_vSPE.end(); ++ s)
            if (speid == s->m_uiID)
               break;

         if (s->m_iStatus == -1)
            continue;

         int progress = *(int32_t*)(msg.getData() + 4);
         gettimeofday(&s->m_LastUpdateTime, 0);
         if (progress < 0)
         {
            //error, quit this segment on the SPE
            s->m_pDS->m_iStatus = 0;
            s->m_pDS->m_iSPEID = -1;
            s->m_iStatus = 0;
            continue;
         }
         if (progress > s->m_iProgress)
            s->m_iProgress = progress;
         if (progress < 100)
            continue;

         self->readResult(&(*s));

         // one SPE completes!
         timeval t;
         gettimeofday(&t, 0);
         self->m_iAvgRunTime = (self->m_iAvgRunTime * 7 + (t.tv_sec - s->m_StartTime.tv_sec)) / 8;
      }
      else
      {
         int bucketid = id;
         vector<BUCKET>::iterator b = self->m_vBucket.begin();
         for (; b != self->m_vBucket.end(); ++ b)
            if (bucketid == b->m_iID)
               break;
         if (b == self->m_vBucket.end())
            continue;
         b->m_iProgress = 100;
      }
   }

   pthread_mutex_unlock(&self->m_RunLock);

   return NULL;
}

int SphereProcess::checkSPE()
{
   timeval t;
   gettimeofday(&t, 0);

   for (vector<SPE>::iterator s = m_vSPE.begin(); s != m_vSPE.end(); ++ s)
   {
      if (-1 == s->m_iStatus)
         continue;

      if (0 == s->m_iStatus)
      {
         Address sn;
         sn.m_strIP = s->m_strIP;
         sn.m_iPort = s->m_iPort;

         // find a new DS and start it
         pthread_mutex_lock(&m_DSLock);

         vector<DS*>::iterator dss = m_vpDS.end();

         for (vector<DS*>::iterator d = m_vpDS.begin(); d != m_vpDS.end(); ++ d)
         {
            if ((0 != (*d)->m_iStatus) || (-1 != (*d)->m_iSPEID))
               continue;

            // if a file is processed via pass by filename, it must be processed on its original location
            // also, this depends on if the source data is allowed to move
            if ((0 != m_iRows) && (m_bDataMove))
               dss = d;

            if ((*d)->m_pLoc->find(sn) != (*d)->m_pLoc->end())
            {
               dss = d;
               break;
            }
         }

         if (dss != m_vpDS.end())
            startSPE(*s, *dss);

         pthread_mutex_unlock(&m_DSLock);
      }
      else 
      {
         int rtime = t.tv_sec - s->m_StartTime.tv_sec;
         int utime = t.tv_sec - s->m_LastUpdateTime.tv_sec;

         if ((rtime > 8 * m_iAvgRunTime) && (utime > 600))
         {
            cerr << "SPE timeout " << s->m_strIP << endl;

            // dismiss this SPE and release its job
            s->m_pDS->m_iStatus = 0;
            s->m_pDS->m_iSPEID = -1;
            s->m_iStatus = -1;
            s->m_DataChn.close();

            m_iTotalSPE --;
         }
      }
   }

   return m_iTotalSPE;
}

int SphereProcess::checkBucket()
{
   int count = 0;
   for (vector<BUCKET>::iterator b = m_vBucket.begin(); b != m_vBucket.end(); ++ b)
   {
      if (b->m_iProgress == 100)
         count ++;
   }

   return m_vBucket.size() - count;
}

int SphereProcess::startSPE(SPE& s, DS* d)
{
   int res = 0;

   if (s.m_iStatus == -1)
   {
      // start an SPE at real time
      if (connectSPE(s) < 0)
         return -1;
   }

   pthread_mutex_lock(&m_ResLock);

   s.m_pDS = d;

   int32_t size = 20 + s.m_pDS->m_strDataFile.length() + 1;
   char* dataseg = new char[size];

   *(int64_t*)(dataseg) = s.m_pDS->m_llOffset;
   *(int64_t*)(dataseg + 8) = s.m_pDS->m_llSize;
   *(int32_t*)(dataseg + 16) = s.m_pDS->m_iID;
   strcpy(dataseg + 20, s.m_pDS->m_strDataFile.c_str());

   if ((s.m_DataChn.send((char*)&size, 4) > 0) && (s.m_DataChn.send(dataseg, size) > 0))
   {
      d->m_iSPEID = s.m_uiID;
      d->m_iStatus = 1;
      s.m_iStatus = 1;
      s.m_iProgress = 0;
      gettimeofday(&s.m_StartTime, 0);
      gettimeofday(&s.m_LastUpdateTime, 0);
      res = 1;
   }

   delete [] dataseg;

   pthread_mutex_unlock(&m_ResLock);

   return res;
}

int SphereProcess::checkProgress()
{
   if (0 == m_iTotalSPE)
      return -1;

   return m_iProgress * 100 / m_iTotalDS;
}

int SphereProcess::checkMapProgress()
{
   return checkProgress();
}

int SphereProcess::checkReduceProgress()
{
   if (m_vBucket.empty())
      return 100;

   int count = 0;
   for (vector<BUCKET>::iterator b = m_vBucket.begin(); b != m_vBucket.end(); ++ b)
   {
      if (b->m_iProgress == 100)
         count ++;
   }

   return count * 100 / m_vBucket.size();   
}

int SphereProcess::read(SphereResult*& res, const bool& inorder, const bool& wait)
{
   while (0 == m_iAvailRes)
   {
      if (!wait || (m_iProgress == m_iTotalDS) || (0 == m_iTotalSPE))
         return -1;

      struct timeval now;
      struct timespec timeout;

      gettimeofday(&now, 0);
      timeout.tv_sec = now.tv_sec + 10;
      timeout.tv_nsec = now.tv_usec * 1000;

      pthread_mutex_lock(&m_ResLock);
      int retcode = pthread_cond_timedwait(&m_ResCond, &m_ResLock, &timeout);
      pthread_mutex_unlock(&m_ResLock);

      if (retcode == ETIMEDOUT)
         return SectorError::E_TIMEDOUT;
   }

   for (vector<DS*>::iterator i = m_vpDS.begin(); i != m_vpDS.end(); ++ i)
   {
      switch ((*i)->m_iStatus)
      {
      case 0:
      case 1:
	 // TODO: fix this bug. inorder check & m_iAvailRes 
         if (inorder)
            return -1;
         break;

      case 2:
         res = (*i)->m_pResult;
         (*i)->m_pResult = NULL;
         res->m_strOrigFile = (*i)->m_strDataFile;

         pthread_mutex_lock(&m_DSLock);
         delete *i;
         m_vpDS.erase(i);
         pthread_mutex_unlock(&m_DSLock);

         pthread_mutex_lock(&m_ResLock);
         -- m_iAvailRes;
         pthread_mutex_unlock(&m_ResLock);

         return 1;

      case 3:
         break;

      default:
         cerr << "unknown error occurs!\n";
      }
   }

   return -1;
}

int SphereProcess::prepareSPE(const char* spenodes)
{
   for (int i = 0; i < m_iSPENum; ++ i)
   {
      // start multiple SPEs per node
      for (int j = 0; j < m_iCore; ++ j)
      {
         SPE spe;
         spe.m_uiID = i * m_iCore + j;
         spe.m_pDS = NULL;
         spe.m_iStatus = -1;
         spe.m_iProgress = 0;
         spe.m_iShufflerPort = 0;

         spe.m_strIP = spenodes + i * 68;
         spe.m_iPort = *(int32_t*)(spenodes + i * 68 + 64);

         m_vSPE.insert(m_vSPE.end(), spe);
      }
   }

   return m_vSPE.size();
}

int SphereProcess::connectSPE(SPE& s)
{
   if (s.m_iStatus >= 0)
      return 0;

   int port = g_iReusePort;
   s.m_DataChn.open(port, true, true);
   g_iReusePort = port;

   SectorMsg msg;
   msg.setType(203); // start processing engine
   msg.setKey(g_iKey);
   msg.setData(0, s.m_strIP.c_str(), s.m_strIP.length() + 1);
   msg.setData(64, (char*)&(s.m_iPort), 4);
   msg.setData(68, (char*)&(s.m_uiID), 4);
   msg.setData(72, (char*)&port, 4);
   msg.setData(76, (char*)&g_iKey, 4);
   msg.setData(80, m_strOperator.c_str(), m_strOperator.length() + 1);
   int offset = 80 + m_strOperator.length() + 1;
   msg.setData(offset, (char*)&m_iRows, 4);
   msg.setData(offset + 4, (char*)&m_iParamSize, 4);
   msg.setData(offset + 8, m_pcParam, m_iParamSize);
   offset += 4 + 8 + m_iParamSize;
   msg.setData(offset, (char*)&m_iProcType, 4);

   if ((g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0) || (msg.getType() < 0))
   {
      cerr << "failed: " << s.m_strIP << " " << s.m_iPort << endl;
      s.m_DataChn.close();
      return -1;
   }

   cout << "connect SPE " << s.m_strIP.c_str() << " " << *(int*)(msg.getData()) << endl;
   if (s.m_DataChn.connect(s.m_strIP.c_str(), *(int*)(msg.getData())) < 0)
   {
      s.m_DataChn.close();
      return -1;
   }

   // send output information
   if (m_iOutputType > 0)
      s.m_DataChn.send(m_pOutputLoc, 4 + m_pOutput->m_iFileNum * 72);
   else if (m_iOutputType < 0)
   {
      s.m_DataChn.send(m_pOutputLoc, 4);
      int size = strlen(m_pOutputLoc + 4) + 1;
      s.m_DataChn.send((char*)&size, 4);
      s.m_DataChn.send(m_pOutputLoc + 4, size);
   }
   else
      s.m_DataChn.send(m_pOutputLoc, 4);

   loadOperator(s);

   s.m_iStatus = 0;

   return 1;
}

int SphereProcess::segmentData()
{
   if (0 == m_iRows)
   {
      int seq = 0;
      for (int i = 0; i < m_pInput->m_iFileNum; ++ i)
      {
         DS* ds = new DS;
         ds->m_iID = seq ++;
         ds->m_strDataFile = m_pInput->m_vFiles[i];
         ds->m_llOffset = 0;
         ds->m_llSize = m_pInput->m_vRecNum[i];
         ds->m_iSPEID = -1;
         ds->m_iStatus = 0;
         ds->m_pLoc = &m_pInput->m_vLocation[i];
         ds->m_pResult = new SphereResult;

         m_vpDS.insert(m_vpDS.end(), ds);
      }
   }
   else if (m_pInput->m_llRecNum != -1)
   {
      int64_t avg = m_pInput->m_llSize / m_iSPENum;
      int64_t unitsize;
      if (avg > m_iMaxUnitSize)
      {
         int n = m_pInput->m_llSize / m_iMaxUnitSize;
         if (m_pInput->m_llSize % m_iMaxUnitSize != 0)
            n ++;
         unitsize = m_pInput->m_llRecNum / n;
      }
      else if (avg < m_iMinUnitSize)
      {
         int n = m_pInput->m_llSize / m_iMinUnitSize;
         if (m_pInput->m_llSize % m_iMinUnitSize != 0)
            n ++;
         unitsize = m_pInput->m_llRecNum / n;
      }
      else
         unitsize = m_pInput->m_llRecNum / m_iSPENum;

      int seq = 0;
      for (int i = 0; i < m_pInput->m_iFileNum; ++ i)
      {
         int64_t off = 0;
         while (off < m_pInput->m_vRecNum[i])
         {
            DS* ds = new DS;
            ds->m_iID = seq ++;
            ds->m_strDataFile = m_pInput->m_vFiles[i];
            ds->m_llOffset = off;
            ds->m_llSize = (m_pInput->m_vRecNum[i] - off > unitsize) ? unitsize : (m_pInput->m_vRecNum[i] - off);
            ds->m_iSPEID = -1;
            ds->m_iStatus = 0;
            ds->m_pLoc = &m_pInput->m_vLocation[i];
            ds->m_pResult = new SphereResult;

            m_vpDS.insert(m_vpDS.end(), ds);

            off += ds->m_llSize;
         }
      }
   }
   else
   {
      cerr << "You have specified the number of records to be processed each time, but there is no record index found.\n";
      return -1;
   }

   return m_vpDS.size();
}

int SphereProcess::prepareOutput()
{
   char* outputloc = NULL;

   // prepare output stream locations
   if (m_iOutputType > 0)
   {
      SectorMsg msg;

      outputloc = new char[m_pOutput->m_iFileNum * 72];
      vector<SPE>::iterator s = m_vSPE.begin();
      int id = -1;

      for (int i = 0; i < m_pOutput->m_iFileNum; ++ i)
      {
         char* tmp = new char[m_pOutput->m_strPath.length() + m_pOutput->m_strName.length() + 64];
         sprintf(tmp, "%s/%s.%d", m_pOutput->m_strPath.c_str(), m_pOutput->m_strName.c_str(), i);
         m_pOutput->m_vFiles[i] = tmp;
         delete [] tmp;

         Address loc;
         loc.m_strIP = s->m_strIP;
         loc.m_iPort = s->m_iPort;
         m_pOutput->m_vLocation[i].insert(loc);

         // start one shuffler on the SPE, if there is no shuffler yet
         if (0 == s->m_iShufflerPort)
         {
            msg.setType(204);
            msg.setKey(g_iKey);

            msg.setData(0, loc.m_strIP.c_str(), loc.m_strIP.length() + 1);
            msg.setData(64, (char*)&(loc.m_iPort), 4);
            msg.setData(68, (char*)&(m_pOutput->m_iFileNum), 4);
            msg.setData(72, (char*)&id, 4);
            int size = m_pOutput->m_strPath.length() + 1;
            int offset = 76;
            msg.setData(offset, (char*)&size, 4);
            msg.setData(offset + 4, m_pOutput->m_strPath.c_str(), m_pOutput->m_strPath.length() + 1);
            offset += 4 + size;
            size = m_pOutput->m_strName.length() + 1;
            msg.setData(offset, (char*)&size, 4);
            msg.setData(offset + 4, m_pOutput->m_strName.c_str(), m_pOutput->m_strName.length() + 1);
            offset += 4 + size;
            msg.setData(offset, (char*)&g_iKey, 4);
            offset += 4;
            msg.setData(offset, (char*)&m_iProcType, 4);
            if (m_iProcType == 1)
            {
               offset += 4;
               size = m_strOperator.length() + 1;
               msg.setData(offset, (char*)&size, 4);
               msg.setData(offset + 4, m_strOperator.c_str(), m_strOperator.length() + 1);
            }

            cout << "request shuffler " << loc.m_strIP << " " << loc.m_iPort << endl;
            if ((g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0) || (msg.getType() < 0))
               continue;

            s->m_iShufflerPort = *(int32_t*)msg.getData();

            BUCKET b;
            b.m_iID = id --;
            b.m_strIP = loc.m_strIP;
            b.m_iPort = *(int32_t*)msg.getData();
            b.m_iProgress = 0;
            gettimeofday(&b.m_LastUpdateTime, 0);
            m_vBucket.insert(m_vBucket.end(), b);
         }

         memcpy(outputloc + i * 72, s->m_strIP.c_str(), 64);
         *(int32_t*)(outputloc + i * 72 + 64) = s->m_iPort;
         *(int32_t*)(outputloc + i * 72 + 68) = s->m_iShufflerPort;

         if (++ s == m_vSPE.end())
            s = m_vSPE.begin();
      }
   }

   // prepare and submit output locations
   int size;
   if (m_iOutputType > 0)
      size = 4 + m_pOutput->m_iFileNum * 72;
   else if (m_iOutputType < 0)
      size = 4 + m_pOutput->m_strPath.length() + m_pOutput->m_strName.length() + 64;
   else
      size = 4;
   m_pOutputLoc = new char[size];

   *(int32_t*)m_pOutputLoc = m_iOutputType;

   if (m_iOutputType > 0)
      memcpy(m_pOutputLoc + 4, outputloc, m_pOutput->m_iFileNum * 72);
   else if (m_iOutputType < 0)
   {
      char* localname = new char[m_pOutput->m_strPath.length() + m_pOutput->m_strName.length() + 64];
      sprintf(localname, "%s/%s", m_pOutput->m_strPath.c_str(), m_pOutput->m_strName.c_str());
      memcpy(m_pOutputLoc + 4, localname, strlen(localname) + 1);
   }

   delete [] outputloc;

   return m_pOutput->m_iFileNum;
}

int SphereProcess::readResult(SPE* s)
{
   s->m_pDS->m_pResult->m_iResID = s->m_pDS->m_iID;
   s->m_pDS->m_pResult->m_strOrigFile = s->m_pDS->m_strDataFile;
   s->m_pDS->m_pResult->m_strIP = s->m_strIP;
   s->m_pDS->m_pResult->m_iPort = s->m_iPort;

   if (m_iOutputType == 0)
   {
      s->m_DataChn.recv((char*)&s->m_pDS->m_pResult->m_iDataLen, 4);
      s->m_pDS->m_pResult->m_pcData = new char[s->m_pDS->m_pResult->m_iDataLen];
      s->m_DataChn.recv(s->m_pDS->m_pResult->m_pcData, s->m_pDS->m_pResult->m_iDataLen);
      s->m_DataChn.recv((char*)&s->m_pDS->m_pResult->m_iIndexLen, 4);
      s->m_pDS->m_pResult->m_pllIndex = new int64_t[s->m_pDS->m_pResult->m_iIndexLen];
      s->m_DataChn.recv((char*)s->m_pDS->m_pResult->m_pllIndex, s->m_pDS->m_pResult->m_iIndexLen * 8);
   }
   else if (m_iOutputType == -1)
   {
      int size = 0;
      s->m_DataChn.recv((char*)&size, 4);
      m_pOutput->m_vSize[s->m_pDS->m_iID] = size;
      m_pOutput->m_llSize += size;

      s->m_DataChn.recv((char*)&size, 4);
      m_pOutput->m_vRecNum[s->m_pDS->m_iID] = size - 1;
      m_pOutput->m_llRecNum += size -1;

      if (m_pOutput->m_iFileNum < 0)
         m_pOutput->m_iFileNum = 1;
      else
         m_pOutput->m_iFileNum ++;
   }
   else
   {
      int* sarray = new int[m_pOutput->m_iFileNum];
      int* rarray = new int[m_pOutput->m_iFileNum];
      s->m_DataChn.recv((char*)sarray, m_pOutput->m_iFileNum * 4);
      s->m_DataChn.recv((char*)rarray, m_pOutput->m_iFileNum * 4);
      for (int i = 0; i < m_pOutput->m_iFileNum; ++ i)
      {
         m_pOutput->m_vSize[i] += sarray[i];
         m_pOutput->m_vRecNum[i] += rarray[i];
         m_pOutput->m_llSize += sarray[i];
         m_pOutput->m_llRecNum += rarray[i];
      }

      delete [] sarray;
      delete [] rarray;
   }

   s->m_pDS->m_iStatus = 2;
   s->m_iStatus = 0;
   ++ m_iProgress;
   pthread_mutex_lock(&m_ResLock);
   ++ m_iAvailRes;
   pthread_cond_signal(&m_ResCond);
   pthread_mutex_unlock(&m_ResLock);

   return 1;
}

int SphereProcess::start()
{
   int totalnum = (m_vSPE.size() < m_vpDS.size()) ? m_vSPE.size() : m_vpDS.size();
   if (0 == totalnum)
      return 0;

   int num = 0;

   for (vector<SPE>::iterator i = m_vSPE.begin(); i != m_vSPE.end(); ++ i)
   {
      vector<DS*>::iterator dss = m_vpDS.end();

      for (vector<DS*>::iterator d = m_vpDS.begin(); d != m_vpDS.end(); ++ d)
      {
         if ((*d)->m_iStatus != 0)
            continue;

         if (0 != m_iRows)
            dss = d;

         Address sn;
         sn.m_strIP = i->m_strIP;
         sn.m_iPort = i->m_iPort;

         if ((*d)->m_pLoc->find(sn) != (*d)->m_pLoc->end())
         {
            dss = d;
            break;
         }
      }

      if (dss == m_vpDS.end())
         continue;

      i->m_pDS = *dss;

      startSPE(*i, i->m_pDS);

      if (++ num == totalnum)
         break;
   }

   return num;
}
