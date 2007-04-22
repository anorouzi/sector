#include "speclient.h"
#include <file.h>

using namespace cb;

Process* SPEClient::createJob()
{
   Process* process = new Process;

   process->m_pSPEClient = this;

   return process;
}

int SPEClient::releaseJob(Process* process)
{
   delete process;
   return 0;
}

Process::Process()
{
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

int Process::open(STREAM stream, string op, const char* param, const int& size)
{
   Node nf, no;

   if (m_pSPEClient->lookup(stream.m_strDataFile, &nf) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(101); // stat
   msg.setData(0, stream.m_strDataFile.c_str(), stream.m_strDataFile.length() + 1);
   msg.m_iDataLength = 4 + stream.m_strDataFile.length() + 1;

   if (m_GMP.rpc(m_pSPEClient->m_strServerHost.c_str(), m_pSPEClient->m_iServerPort, &msg, &msg) < 0)
      return -1;

   CFileAttr attr;
   if (msg.getType() > 0)
      attr.deserialize(msg.getData(), msg.m_iDataLength - 4);

   int64_t totalUnits = attr.m_llSize / stream.m_iUnitSize;

   if (m_pSPEClient->lookup(op + ".so", &no) < 0)
      return -1;

   msg.setType(1); // locate file
   msg.setData(0, (op + ".so").c_str(), (op + ".so").length() + 1);
   msg.m_iDataLength = 4 + (op + ".so").length() + 1;

   if ((m_GMP.rpc(no.m_pcIP, no.m_iAppPort, &msg, &msg) < 0) || (msg.getType() < 0))
      return -1;

   SPE spe;
   int n = (msg.m_iDataLength - 4) / 68;
   int64_t unitsize = totalUnits / n * stream.m_iUnitSize;
   for (int i = 0; i < n; ++ i)
   {
      spe.m_uiID = i;

      strcpy(spe.m_Loc.m_pcIP, msg.getData() + i * 68);
      spe.m_Loc.m_iPort = *(int32_t*)(msg.getData() + i * 68 + 64);
      spe.m_llOffset = i * unitsize;
      spe.m_llSize = (i != n - 1) ? unitsize : totalUnits * stream.m_iUnitSize - spe.m_llOffset;
      spe.m_iUnitSize = stream.m_iUnitSize;

      spe.m_strDataFile = stream.m_strDataFile;
      spe.m_strOperator = op;
      spe.m_strOperator = op;

      spe.m_iStatus = 0;

      if (NULL != param)
      {
         spe.m_pcParam = new char [size];
         memcpy(spe.m_pcParam, param, size);
         spe.m_iParamSize = size;
      }
      else
      {
         spe.m_pcParam = NULL;
         spe.m_iParamSize = 0;
      }

      m_vSPE.insert(m_vSPE.end(), spe);
   }

   cout << m_vSPE.size() << " spes found!" << endl;

   return 0;
}

int Process::close()
{
   return 0;
}

int Process::run()
{
   CCBMsg msg;

   for (vector<SPE>::iterator i = m_vSPE.begin(); i != m_vSPE.end(); ++ i)
   {
      i->m_DataSock = UDT::socket(AF_INET, SOCK_STREAM, 0);

      sockaddr_in my_addr;
      my_addr.sin_family = AF_INET;
      my_addr.sin_port = 0;
      my_addr.sin_addr.s_addr = INADDR_ANY;
      memset(&(my_addr.sin_zero), '\0', 8);

      UDT::bind(i->m_DataSock, (sockaddr*)&my_addr, sizeof(my_addr));

      int size = sizeof(sockaddr_in);
      UDT::getsockname(i->m_DataSock, (sockaddr*)&my_addr, &size);

      msg.setType(300); // start processing engine
      msg.setData(0, i->m_strDataFile.c_str(), i->m_strDataFile.length() + 1);
      msg.setData(64, i->m_strOperator.c_str(), i->m_strOperator.length() + 1);
      msg.setData(128, (char*)&(i->m_uiID), 4);
      msg.setData(132, (char*)&(i->m_llOffset), 8);
      msg.setData(140, (char*)&(i->m_llSize), 8);
      msg.setData(148, (char*)&(i->m_iUnitSize), 4);
      msg.setData(152, (char*)&(i->m_iParamSize), 4);
      msg.setData(156, (char*)&(my_addr.sin_port), 4);
      if (i->m_iParamSize > 0)
         msg.setData(160, i->m_pcParam, i->m_iParamSize);
      msg.m_iDataLength = 4 + 160 + i->m_iParamSize;

      if (m_GMP.rpc(i->m_Loc.m_pcIP, i->m_Loc.m_iPort, &msg, &msg) < 0)
      {
         i->m_iStatus = -1;
         continue;
      }

      sockaddr_in serv_addr;
      serv_addr.sin_family = AF_INET;
      serv_addr.sin_port = *(int*)(msg.getData()); // port
      inet_pton(AF_INET, i->m_Loc.m_pcIP, &serv_addr.sin_addr);
      memset(&(serv_addr.sin_zero), '\0', 8);

cout << "UDT connecting " <<  i->m_Loc.m_pcIP << " " << *(int*)(msg.getData()) << endl;

      int rendezvous = 1;
      UDT::setsockopt(i->m_DataSock, 0, UDT_RENDEZVOUS, &rendezvous, 4);

      if (UDT::ERROR == UDT::connect(i->m_DataSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      {
         i->m_iStatus = -1;
         continue;
      }

      i->m_iStatus = 1;

      pthread_t reduce;
      pthread_create(&reduce, NULL, run, this);
      pthread_detach(reduce);
   }

   return 0;
}

void* Process::run(void* param)
{
   Process* self = (Process*)param;

   char ip[64];
   int port;
   int id;
   CCBMsg* msg = new CCBMsg;

   while (true)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      cout << "recv CB " << msg->getType() << " " << ip << " " << port << endl;

      uint32_t speid = *(uint32_t*)(msg->getData());
      vector<SPE>::iterator s = self->m_vSPE.begin();
      for (; s != self->m_vSPE.end(); ++ s)
         if (speid == s->m_uiID)
            break;

      cout << s->m_uiID << " " << speid << endl;

      int size = *(int32_t*)(msg->getData() + 4);

      msg->m_iDataLength = 4;
      self->m_GMP.sendto(ip, port, id, msg);

      cout << "result is back!!! " << size << endl;

      char* data = new char[size];

      int h;
      UDT::recv(s->m_DataSock, data, size, 0, &h);

cout << "got it\n";

      pthread_mutex_lock(&self->m_ResLock);
      Result res;
      res.m_pcRes = data;
      res.m_iSize = size;
      self->m_vResult.insert(self->m_vResult.end(), res);
      pthread_cond_signal(&self->m_ResCond);
      pthread_mutex_unlock(&self->m_ResLock);
   }

   return NULL;
}

int Process::read(char*& data, int& size)
{
   pthread_mutex_lock(&m_ResLock);

   if (m_vResult.empty())
      pthread_cond_wait(&m_ResCond, &m_ResLock);

   data = m_vResult.begin()->m_pcRes;
   size = m_vResult.begin()->m_iSize;
   m_vResult.erase(m_vResult.begin());

   pthread_mutex_unlock(&m_ResLock);

   return 0;
}
