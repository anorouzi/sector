/*****************************************************************************
Copyright � 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/24/2008
*****************************************************************************/

#include "dcclient.h"
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

int SphereStream::init(const vector<string>& files)
{
   m_iFileNum = files.size();
   if (0 == m_iFileNum)
      return 0;

   m_iStatus = -1;

   m_vFiles.resize(m_iFileNum);
   copy(files.begin(), files.end(), m_vFiles.begin());

   m_vSize.resize(m_iFileNum);
   m_vRecNum.resize(m_iFileNum);
   vector<int64_t>::iterator s = m_vSize.begin();
   vector<int64_t>::iterator r = m_vRecNum.begin();

   m_vLocation.resize(m_iFileNum);
   int lp = 0;

   bool indexfound = true;

   for (vector<string>::iterator i = m_vFiles.begin(); i != m_vFiles.end(); ++ i)
   {
      SNode fattr, iattr;
      if (Client::stat(*i, fattr) < 0)
         return -1;

      *s = fattr.m_llSize;
      m_llSize += *s;

      // retrieve file size and index
      if (Client::stat(*i + ".idx", iattr) < 0)
      {
          // no record index found
          *r = -1;
          m_llRecNum = -1;
          indexfound = false;
      }
      else
      {
         if (indexfound)
         {
            *r = iattr.m_llSize / 8 - 1;
            m_llRecNum += *r;
         }
      }

      s ++;
      r ++;

      m_vLocation[lp ++] = fattr.m_sLocation;
   }

   if (!indexfound)
      cout << "warning: no record index found!\n";

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

int SphereStream::setSeg(const int64_t& start, const int64_t& end)
{
   m_llStart = start;
   m_llEnd = end;

   return 0;
}

//
SphereProcess::SphereProcess():
m_iMinUnitSize(1000000),
m_iMaxUnitSize(64000000)
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
      return -1;

   int size = st.st_size;

   ifstream lib;
   lib.open(library, ios::binary);
   if (lib.bad())
      return -1;

   char* buf = new char[size];
   lib.read(buf, size);
   lib.close();

   vector<string> dir;
   if (Index::parsePath(library, dir) < 0)
   {
      delete [] buf;
      return 0;
   }

   SectorMsg msg;
   msg.setType(201);
   msg.setKey(m_iKey);
   msg.resize(size + SectorMsg::m_iHdrSize + 64);
   msg.setData(0, dir[dir.size() - 1].c_str(), dir[dir.size() - 1].length() + 1);
   msg.setData(64, buf, size);

   delete [] buf;

   cout << "load op " << m_strServerIP << " " << m_iServerPort << endl;

   int r = m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg);

   if ((r < 0) || (msg.getType() < 0))
      return -1;

   return 0;
}

int SphereProcess::run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param, const int& size)
{
   pthread_mutex_lock(&m_RunLock);
   pthread_mutex_unlock(&m_RunLock);

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
   msg.setKey(m_iKey);
   msg.m_iDataLength = SectorMsg::m_iHdrSize;

   if ((m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0) || (msg.getType() < 0))
      return -1;

   m_iSPENum = (msg.m_iDataLength - 4) / 68;
   if (0 == m_iSPENum)
      return -1;

   cout << m_iSPENum << " SPE found" << endl;

   prepareSPE(msg.getData());

   if (segmentData() <= 0)
      return -1;

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

   while (self->m_iProgress < self->m_iTotalDS)
   {
      if (0 == self->checkSPE())
         break;

      char ip[64];
      int port;
      int id;
      SectorMsg msg;
      if (self->m_GMP.recvfrom(ip, port, id, &msg, false) < 0)
         continue;

      uint32_t speid = *(uint32_t*)(msg.getData());
      vector<SPE>::iterator s = self->m_vSPE.begin();
      for (; s != self->m_vSPE.end(); ++ s)
         if (speid == s->m_uiID)
            break;

      if (s->m_iStatus == -1)
         continue;

      int progress = *(int32_t*)(msg.getData() + 4);
      gettimeofday(&s->m_LastUpdateTime, 0);
      if (progress <= s->m_iProgress)
         continue;
      s->m_iProgress = progress;
      if (progress < 100)
         continue;

      self->readResult(&(*s));

      // one SPE completes!
      timeval t;
      gettimeofday(&t, 0);
      self->m_iAvgRunTime = (self->m_iAvgRunTime * 7 + (t.tv_sec - s->m_StartTime.tv_sec)) / 8;
   }

   // disconnect all SPEs
   for (vector<SPE>::iterator i = self->m_vSPE.begin(); i != self->m_vSPE.end(); ++ i)
      i->m_DataChn.close();

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
            if (0 != m_iRows)
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

int SphereProcess::startSPE(SPE& s, DS* d)
{
   int res = 0;

   pthread_mutex_lock(&m_ResLock);

   s.m_pDS = d;

   char* dataseg = new char[80];

   strcpy(dataseg, s.m_pDS->m_strDataFile.c_str());
   *(int64_t*)(dataseg + 64) = s.m_pDS->m_llOffset;
   *(int64_t*)(dataseg + 72) = s.m_pDS->m_llSize;

   if (s.m_DataChn.send(dataseg, 80) > 0)
   {
      d->m_iSPEID = s.m_uiID;
      d->m_iStatus = 1;
      s.m_iStatus = 1;
      s.m_iProgress = 0;
      gettimeofday(&s.m_StartTime, 0);
      gettimeofday(&s.m_LastUpdateTime, 0);
      res = 1;
   }

   pthread_mutex_unlock(&m_ResLock);

   return res;
}

int SphereProcess::checkProgress()
{
   if (0 == m_iTotalSPE)
      return -1;

   return m_iProgress * 100 / m_iTotalDS;
}

int SphereProcess::read(SphereResult*& res, const bool& inorder, const bool& wait)
{
   while (0 == m_iAvailRes)
   {
      if (!wait || (m_iProgress == m_iTotalDS) || (0 == m_iTotalSPE))
         return -1;

      pthread_mutex_lock(&m_ResLock);
      pthread_cond_wait(&m_ResCond, &m_ResLock);
      pthread_mutex_unlock(&m_ResLock);
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
      int core = 1;
      for (int j = 0; j < core; ++ j)
      {
         SPE spe;
         spe.m_uiID = i * core + j;
         spe.m_pDS = NULL;
         spe.m_iStatus = 0;
         spe.m_iProgress = 0;
         spe.m_iShufflerPort = 0;

         spe.m_strIP = spenodes + i * 68;
         spe.m_iPort = *(int32_t*)(spenodes + i * 68 + 64);

         int port = 0;
         spe.m_DataChn.open(port);

         SectorMsg msg;
         msg.setType(203); // start processing engine
         msg.setKey(m_iKey);
         msg.setData(0, spe.m_strIP.c_str(), spe.m_strIP.length() + 1);
         msg.setData(64, (char*)&(spe.m_iPort), 4);
         msg.setData(68, (char*)&(spe.m_uiID), 4);
         msg.setData(72, (char*)&port, 4);
         msg.setData(76, (char*)&m_iKey, 4);
         msg.setData(80, m_strOperator.c_str(), m_strOperator.length() + 1);
         msg.setData(144, (char*)&m_iRows, 4);
         msg.setData(148, (char*)&m_iParamSize, 4);
         msg.setData(152, m_pcParam, m_iParamSize);
         msg.m_iDataLength = SectorMsg::m_iHdrSize + 152 + m_iParamSize;

         if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
         {
            cout << "failed: " << spe.m_strIP << " " << spe.m_iPort << endl;
            spe.m_DataChn.close();
            continue;
         }
         cout << "connect SPE " << spe.m_strIP.c_str() << " " << *(int*)(msg.getData()) << endl;

         if (spe.m_DataChn.connect(spe.m_strIP.c_str(), *(int*)(msg.getData())) < 0)
         {
            spe.m_DataChn.close();
            continue;
         }

         m_vSPE.insert(m_vSPE.end(), spe);
      }
   }

   return m_vSPE.size();
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
         unitsize = m_iMaxUnitSize * m_pInput->m_llRecNum / m_pInput->m_llSize;
      else if (avg < m_iMinUnitSize)
         unitsize = m_iMinUnitSize * m_pInput->m_llRecNum / m_pInput->m_llSize;
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
      // data segementation error
      return -1;
   }

   return m_vpDS.size();
}

int SphereProcess::prepareOutput()
{
   // prepare output stream locations
   m_pOutputLoc = NULL;
   if (m_iOutputType > 0)
   {
      SectorMsg msg;

      m_pOutputLoc = new char[m_pOutput->m_iFileNum * 72];

      vector<SPE>::iterator s = m_vSPE.begin();

      for (int i = 0; i < m_pOutput->m_iFileNum; ++ i)
      {
         char tmp[64];
         sprintf(tmp, "%s/%s.%d", m_pOutput->m_strPath.c_str(), m_pOutput->m_strName.c_str(), i);
         m_pOutput->m_vFiles[i] = tmp;

         Address loc;
         loc.m_strIP = s->m_strIP;
         loc.m_iPort = s->m_iPort;
         m_pOutput->m_vLocation[i].insert(loc);

         // start one shuffler on the SPE, if there is no shuffler yet
         if (0 == s->m_iShufflerPort)
         {
            msg.setType(204);
            msg.setKey(m_iKey);

            msg.setData(0, loc.m_strIP.c_str(), loc.m_strIP.length() + 1);
            msg.setData(64, (char*)&(loc.m_iPort), 4);
            int32_t dsnum = m_vpDS.size();
            msg.setData(68, (char*)&dsnum, 4);
            msg.setData(72, m_pOutput->m_strPath.c_str(), m_pOutput->m_strPath.length() + 1);
            msg.setData(136, m_pOutput->m_strName.c_str(), m_pOutput->m_strName.length() + 1);

            msg.m_iDataLength = SectorMsg::m_iHdrSize + 136 + m_pOutput->m_strName.length() + 1;

            cout << "request shuffler " << loc.m_strIP << " " << loc.m_iPort << endl;

            m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg);

            s->m_iShufflerPort = *(int32_t*)msg.getData();
         }

         memcpy(m_pOutputLoc + i * 72, s->m_strIP.c_str(), 64);
         *(int32_t*)(m_pOutputLoc + i * 72 + 64) = s->m_iPort;
         *(int32_t*)(m_pOutputLoc + i * 72 + 68) = s->m_iShufflerPort;

         if (++ s == m_vSPE.end())
            s = m_vSPE.begin();
      }
   }

   // prepare and submit output locations
   char* outputloc;
   int size;
   if (m_iOutputType > 0)
      size = 4 + m_pOutput->m_iFileNum * 72;
   else if (m_iOutputType < 0)
      size = 4 + 64;
   else
      size = 4;
   outputloc = new char[size];

   *(int32_t*)outputloc = m_iOutputType;

   if (m_iOutputType > 0)
      memcpy(outputloc + 4, m_pOutputLoc, m_pOutput->m_iFileNum * 72);
   else if (m_iOutputType < 0)
   {
      //TODO: fix this
      char localname[64];
      localname[63] = '\0';
      //sprintf(localname, "%s.%d", m_pOutput->m_strName.c_str(), d->m_iID);
      memcpy(outputloc + 4, localname, strlen(localname) + 1);
      //m_pOutput->m_vFiles[d->m_iID] = localname;
   }

   for (vector<SPE>::iterator s = m_vSPE.begin(); s != m_vSPE.end(); ++ s)
   {
      if (s->m_DataChn.send(outputloc, size) < 0)
      {
         s->m_DataChn.close();
         s->m_iStatus = -1;
         return -1;
      }
   }

   return m_pOutput->m_iFileNum;
}

int SphereProcess::readResult(SPE* s)
{
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