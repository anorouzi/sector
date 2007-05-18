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
m_iMinUnitSize(8000000),
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

   int n = (msg.m_iDataLength - 4) / 68;
   int64_t avg = totalSize / n;
   int64_t unitsize;
   if (avg > m_iMaxUnitSize)
      unitsize = m_iMaxUnitSize * totalRec / totalSize;
   else if (avg < m_iMinUnitSize)
      unitsize = m_iMinUnitSize * totalRec / totalSize;
   else
      unitsize = totalRec / n;

   // data segment
   for (int i = 0; i < stream.size(); ++ i)
   {
      int64_t off = 0;
      while (off < arec[i])
      {
         DS ds;
         ds.m_strDataFile = stream[i];
         ds.m_llOffset = off;
         ds.m_llSize = (arec[i] - off > unitsize) ? unitsize : (arec[i] - off);
         ds.m_iSPEID = -1;
         ds.m_pResult = NULL;
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

      strcpy(spe.m_Loc.m_pcIP, msg.getData() + i * 68);
      spe.m_Loc.m_iPort = *(int32_t*)(msg.getData() + i * 68 + 64);

      spe.m_DataSock = UDT::socket(AF_INET, SOCK_STREAM, 0);

      sockaddr_in my_addr;
      my_addr.sin_family = AF_INET;
      my_addr.sin_port = 0;
      my_addr.sin_addr.s_addr = INADDR_ANY;
      memset(&(my_addr.sin_zero), '\0', 8);

      UDT::bind(spe.m_DataSock, (sockaddr*)&my_addr, sizeof(my_addr));

      int size = sizeof(sockaddr_in);
      UDT::getsockname(spe.m_DataSock, (sockaddr*)&my_addr, &size);

      msg.setType(300); // start processing engine
      msg.setData(0, (char*)&(spe.m_uiID), 4);
      msg.setData(4, (char*)&(my_addr.sin_port), 4);
      msg.setData(8, m_strOperator.c_str(), m_strOperator.length() + 1);
      msg.setData(72, m_strParam.c_str(), m_strParam.length() + 1);

      if (m_GMP.rpc(spe.m_Loc.m_pcIP, spe.m_Loc.m_iPort, &msg, &msg) < 0)
      {
         UDT::close(spe.m_DataSock);
         continue;
      }

      sockaddr_in serv_addr;
      serv_addr.sin_family = AF_INET;
      serv_addr.sin_port = *(int*)(msg.getData()); // port
      inet_pton(AF_INET, spe.m_Loc.m_pcIP, &serv_addr.sin_addr);
      memset(&(serv_addr.sin_zero), '\0', 8);

      cout << "UDT connecting " <<  spe.m_Loc.m_pcIP << " " << *(int*)(msg.getData()) << endl;

      int rendezvous = 1;
      UDT::setsockopt(spe.m_DataSock, 0, UDT_RENDEZVOUS, &rendezvous, 4);

      if (UDT::ERROR == UDT::connect(spe.m_DataSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      {
         UDT::close(spe.m_DataSock);
         continue;
      }

      m_vSPE.insert(m_vSPE.end(), spe);
   }

   m_iProgress = 0;

   cout << m_vSPE.size() << " spes found! " << m_vDS.size() << " data seg total." << endl;

   return 0;
}

int Process::close()
{
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

      char param[80];
      strcpy(param, self->m_vSPE[i].m_pDS->m_strDataFile.c_str());
      *(int64_t*)(param + 64) = self->m_vSPE[i].m_pDS->m_llOffset;
      *(int64_t*)(param + 72) = self->m_vSPE[i].m_pDS->m_llSize;

      if (UDT::send(self->m_vSPE[i].m_DataSock, param, 80, 0) > 0)
      {
         self->m_vDS[i].m_iSPEID = self->m_vSPE[i].m_uiID;
         self->m_vSPE[i].m_iStatus = 1;
      }
   }

   char ip[64];
   int port;
   int id;
   CCBMsg* msg = new CCBMsg;

   while (true)
   {
      if (self->m_GMP.recvfrom(ip, port, id, msg) < 0)
      {
         delete msg;
         return NULL;
      }
      msg->m_iDataLength = 4;
      self->m_GMP.sendto(ip, port, id, msg);

      //cout << "recv CB " << msg->getType() << " " << ip << " " << port << endl;

      uint32_t speid = *(uint32_t*)(msg->getData());
      vector<SPE>::iterator s = self->m_vSPE.begin();
      for (; s != self->m_vSPE.end(); ++ s)
         if (speid == s->m_uiID)
            break;

      int progress = *(int32_t*)(msg->getData() + 4);

      if (progress <= s->m_iProgress)
         continue;

      cout << s->m_uiID << " " << speid << " " << progress << endl;

      s->m_iProgress = progress;

      if (progress < 100)
         continue;

      cout << "result is back!!! " << *(int32_t*)(msg->getData() + 8) << endl;

      s->m_pDS->m_iResSize = *(int32_t*)(msg->getData() + 8);
      s->m_pDS->m_pResult = new char[s->m_pDS->m_iResSize];

      int h;
      if (UDT::ERROR == UDT::recv(s->m_DataSock, s->m_pDS->m_pResult,  s->m_pDS->m_iResSize, 0, &h))
      {
         cout << UDT::getlasterror().getErrorMessage() << endl;
         delete msg;
         return NULL;
      }

      cout << "got it " << speid << endl;

      s->m_iStatus = -1;
      ++ self->m_iProgress;

      if (self->m_iProgress < self->m_vDS.size())
      {
         for (vector<DS>::iterator i = self->m_vDS.begin(); i != self->m_vDS.end(); ++ i)
         {
            if (-1 == i->m_iSPEID)
            {
               s->m_pDS = &(*i);

               char param[80];
               strcpy(param, s->m_pDS->m_strDataFile.c_str());
               *(int64_t*)(param + 64) = s->m_pDS->m_llOffset;
               *(int64_t*)(param + 72) = s->m_pDS->m_llSize;

               if (UDT::send(s->m_DataSock, param, 80, 0) > 0)
               {
                  i->m_iSPEID = s->m_uiID;
                  s->m_iStatus = 1;
               }
            }
         }
      }
      else
         break;
   }

   delete msg;
   return NULL;
}

int Process::read(char*& data, int& size, string& file, int64_t& offset, int& rows, const bool& inorder)
{
   sleep(1000);

   return size;
}
