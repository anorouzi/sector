/*****************************************************************************
Copyright � 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/22/2007
*****************************************************************************/

#include "speclient.h"
#include "fsclient.h"

using namespace cb;

Process* Client::createJob()
{
   Process* process = new Process;

   return process;
}

int Client::releaseJob(Process* process)
{
   delete process;
   return 0;
}

Process::Process():
m_iMinUnitSize(80),
m_iMaxUnitSize(128000000)
{
   m_strOperator = "";
   m_strParam = "";
   m_vstrStream.clear();

   m_GMP.init(0);

   m_vSPE.clear();

   pthread_mutex_init(&m_ResLock, NULL);
   pthread_cond_init(&m_ResCond, NULL);
}

Process::~Process()
{
   m_GMP.close();

   m_vSPE.clear();

   pthread_mutex_destroy(&m_ResLock);
   pthread_cond_destroy(&m_ResCond);
}

int Process::open(vector<string> stream, string op, const char* param, const int& size)
{
   m_strOperator = op;
   char* temp = new char[size + 1];
   memcpy(temp, param, size);
   temp[size] = '\0';
   m_strParam = temp;
   delete [] temp;
   m_vstrStream = stream;

   uint64_t totalSize = 0;
   uint64_t totalRec = 0;

   int* asize = new int[stream.size()];
   int* arec = new int[stream.size()];

   int c = 0;
   for (vector<string>::iterator i = stream.begin(); i != stream.end(); ++ i)
   {
      CFileAttr fattr, iattr;
      if (Client::stat(*i, fattr) < 0)
         return -1;
      if (Client::stat(*i + ".idx", iattr) < 0)
         return -1;

      asize[c] = fattr.m_llSize;
      arec[c] = iattr.m_llSize / 8 - 1;

      totalSize += asize[c];
      totalRec += arec[c];

      ++ c;
   }

   cout << "JOB " << totalSize << " " << totalRec << endl;

   Node no;
   if (Client::lookup(op + ".so", &no) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(1); // locate file
   msg.setData(0, (op + ".so").c_str(), (op + ".so").length() + 1);
   msg.m_iDataLength = 4 + (op + ".so").length() + 1;

   if ((m_GMP.rpc(no.m_pcIP, no.m_iAppPort, &msg, &msg) < 0) || (msg.getType() < 0))
      return -1;

   char* spenodes = new char[msg.m_iDataLength - 4];
   memcpy(spenodes, msg.getData(), msg.m_iDataLength - 4);
   int n = (msg.m_iDataLength - 4) / 68;
   if (0 == n)
      return -1;

   int64_t avg = totalSize / n;
   int64_t unitsize;
   if (avg > m_iMaxUnitSize)
      unitsize = m_iMaxUnitSize * totalRec / totalSize;
   else if (avg < m_iMinUnitSize)
      unitsize = m_iMinUnitSize * totalRec / totalSize;
   else
      unitsize = totalRec / n;

   // data segment
   for (unsigned int i = 0; i < stream.size(); ++ i)
   {
      int64_t off = 0;
      while (off < arec[i])
      {
         DS ds;
         ds.m_strDataFile = stream[i];
         ds.m_llOffset = off;
         ds.m_llSize = (arec[i] - off > unitsize) ? unitsize : (arec[i] - off);
         ds.m_iSPEID = -1;
         ds.m_iStatus = 0;
         ds.m_pcResult = NULL;
         ds.m_iResSize = 0;

         m_vDS.insert(m_vDS.end(), ds);

         off += ds.m_llSize;
      }
   }

   // prepare SPEs
   for (int i = 0; i < n; ++ i)
   {
      SPE spe;
      spe.m_uiID = i;
      spe.m_pDS = NULL;
      spe.m_iStatus = 0;
      spe.m_iProgress = 0;

      spe.m_strIP = spenodes + i * 68;
      spe.m_iPort = *(int32_t*)(spenodes + i * 68 + 64);

      int port = 0;
      spe.m_DataChn.open(port);

      msg.setType(300); // start processing engine
      msg.setData(0, (char*)&(spe.m_uiID), 4);
      msg.setData(4, (char*)&port, 4);
      msg.setData(8, m_strOperator.c_str(), m_strOperator.length() + 1);
      msg.setData(72, m_strParam.c_str(), m_strParam.length() + 1);
      msg.m_iDataLength = 4 + 72 + m_strParam.length() + 1;

      if (m_GMP.rpc(spe.m_strIP.c_str(), spe.m_iPort, &msg, &msg) < 0)
      {
         cout << "failed: " << spe.m_strIP << " " << spe.m_iPort << endl;
         spe.m_DataChn.close();
         continue;
      }

      if (spe.m_DataChn.connect(spe.m_strIP.c_str(), *(int*)(msg.getData())) < 0)
      {
         spe.m_DataChn.close();
         continue;
      }

      m_vSPE.insert(m_vSPE.end(), spe);
   }

   m_iProgress = 0;
   m_iAvailRes = 0;
   delete [] asize;
   delete [] arec;
   delete [] spenodes;

   cout << m_vSPE.size() << " spes found! " << m_vDS.size() << " data seg total." << endl;

   return 0;
}

int Process::close()
{
   for (vector<SPE>::iterator i = m_vSPE.begin(); i != m_vSPE.end(); ++ i)
      i->m_DataChn.close();

   m_vSPE.clear();
   m_vDS.clear();

   return 0;
}

int Process::run()
{
   pthread_t reduce;
   pthread_create(&reduce, NULL, run, this);
   pthread_detach(reduce);

   return 0;
}

void* Process::run(void* param)
{
   Process* self = (Process*)param;

   // start initial round
   int num = (self->m_vSPE.size() < self->m_vDS.size()) ? self->m_vSPE.size() : self->m_vDS.size();
   for (int i = 0; i < num; ++ i)
   {
      self->m_vSPE[i].m_pDS = &self->m_vDS[i];

      char dataseg[80];
      strcpy(dataseg, self->m_vSPE[i].m_pDS->m_strDataFile.c_str());
      *(int64_t*)(dataseg + 64) = self->m_vSPE[i].m_pDS->m_llOffset;
      *(int64_t*)(dataseg + 72) = self->m_vSPE[i].m_pDS->m_llSize;

      if (self->m_vSPE[i].m_DataChn.send(dataseg, 80) > 0)
      {
         self->m_vDS[i].m_iSPEID = self->m_vSPE[i].m_uiID;
         self->m_vDS[i].m_iStatus = 1;
         self->m_vSPE[i].m_iStatus = 1;
         self->m_vSPE[i].m_iProgress = 0;
      }
   }

   while (true)
   {
      char ip[64];
      int port;
      int id;
      CCBMsg msg;
      if (self->m_GMP.recvfrom(ip, port, id, &msg) < 0)
         continue;

      msg.m_iDataLength = 4;
      self->m_GMP.sendto(ip, port, id, &msg);

      //cout << "recv CB " << msg.getType() << " " << ip << " " << port << endl;

      uint32_t speid = *(uint32_t*)(msg.getData());
      vector<SPE>::iterator s = self->m_vSPE.begin();
      for (; s != self->m_vSPE.end(); ++ s)
         if (speid == s->m_uiID)
            break;

      int progress = *(int32_t*)(msg.getData() + 4);

      if (progress <= s->m_iProgress)
         continue;

      cout << s->m_uiID << " " << speid << " " << progress << endl;

      s->m_iProgress = progress;

      if (progress < 100)
         continue;

      cout << "result is back!!! " << *(int32_t*)(msg.getData() + 8) << endl;

      s->m_pDS->m_iResSize = *(int32_t*)(msg.getData() + 8);
      s->m_pDS->m_pcResult = new char[s->m_pDS->m_iResSize];

      if (s->m_DataChn.recv(s->m_pDS->m_pcResult, s->m_pDS->m_iResSize) < 0)
      {
         s->m_iStatus = -1;
         continue;
      }

      cout << "got it " << speid << endl;
      if (s->m_pDS->m_iResSize == 4)
         cout << "LAST BLOCK " << *(int*)s->m_pDS->m_pcResult << endl;

      s->m_pDS->m_iStatus = 2;
      s->m_iStatus = -1;
      ++ self->m_iProgress;
      pthread_mutex_lock(&self->m_ResLock);
      ++ self->m_iAvailRes;
      pthread_cond_signal(&self->m_ResCond);
      pthread_mutex_unlock(&self->m_ResLock);

      if ((unsigned int)self->m_iProgress < self->m_vDS.size())
      {
         for (vector<DS>::iterator i = self->m_vDS.begin(); i != self->m_vDS.end(); ++ i)
         {
            if (-1 == i->m_iSPEID)
            {
               s->m_pDS = &(*i);

               char dataseg[80];
               strcpy(dataseg, s->m_pDS->m_strDataFile.c_str());
               *(int64_t*)(dataseg + 64) = s->m_pDS->m_llOffset;
               *(int64_t*)(dataseg + 72) = s->m_pDS->m_llSize;

               if (s->m_DataChn.send(dataseg, 80) > 0)
               {
                  i->m_iSPEID = s->m_uiID;
                  i->m_iStatus = 1;
                  s->m_iStatus = 1;
                  s->m_iProgress = 0;
               }
            }
         }
      }
      else
         break;
   }

   return NULL;
}

int Process::checkProgress()
{
   return m_iProgress * 100 / m_vDS.size();
}

int Process::read(char*& data, int& size, string& file, int64_t& offset, int& rows, const bool& inorder, const bool& wait)
{
   if (0 == m_iAvailRes)
   {
      if (!wait || ((unsigned int)m_iProgress == m_vDS.size()))
         return -1;

      pthread_mutex_lock(&m_ResLock);
      pthread_cond_wait(&m_ResCond, &m_ResLock);
      pthread_mutex_unlock(&m_ResLock);
   }

   for (vector<DS>::iterator i = m_vDS.begin(); i != m_vDS.end(); ++ i)
   {
      switch (i->m_iStatus)
      {
      case 0:
      case 1:
         if (inorder)
            return -1;
         break;

      case 2:
         data = i->m_pcResult;
         size = i->m_iResSize;
         i->m_pcResult = NULL;
         i->m_iResSize = 0;
         i->m_iStatus = 3;
         -- m_iAvailRes;
         return size;

      case 3:
         break;

      default:
         cerr << "unknown error occurs!\n";
      }
   }

   return -1;
}
