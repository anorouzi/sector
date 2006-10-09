#include <fsclient.h>

using namespace std;


const int CFSClient::m_iCBFSPort = 2237;         //cbfs


CFSClient::CFSClient():
m_iProtocol(1)
{
   m_pGMP = new CGMP;
}

CFSClient::CFSClient(const int& protocol):
m_iProtocol(protocol)
{
   m_pGMP = new CGMP;
}

CFSClient::~CFSClient()
{
   delete m_pGMP;
}

int CFSClient::connect(const string& server, const int& port)
{
   m_strServerHost = server;
   m_iServerPort = port;

   m_pGMP->init(0);

   return 1;
}

int CFSClient::close()
{
   m_pGMP->close();

   return 1;
}

CCBFile* CFSClient::createFileHandle()
{
   CCBFile *f = NULL;

   try
   {
      f = new CCBFile;
   }
   catch (...)
   {
      return NULL;
   }

   f->m_pFSClient = this;
   f->m_iProtocol = m_iProtocol;

   return f;
}

void CFSClient::releaseFileHandle(CCBFile* f)
{
   delete f;
}

int CFSClient::lookup(string filename, Node* n)
{
   CCBMsg msg;
   msg.setType(4); // look up a file server
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.m_iDataLength = 4 + filename.length() + 1;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      memcpy(n, msg.getData(), sizeof(Node)); 

   return msg.getType();
}

int CFSClient::stat(const string& filename, CFileAttr& attr)
{
   CCBMsg msg;
   msg.setType(9); // stat
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.m_iDataLength = 4 + filename.length() + 1;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      attr.desynchronize(msg.getData(), msg.m_iDataLength - 4);

   return msg.getType();
}

int CFSClient::ls(vector<string>& filelist)
{
   CCBMsg msg;
   msg.resize(65536);
   msg.setType(8); // retrieve name index
   *(int32_t*)msg.getData() = 0;
   msg.m_iDataLength = 4 + 4;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      CNameIndex::desynchronize(filelist, msg.getData(), msg.m_iDataLength - 4);

   return filelist.size();
}



CCBFile::CCBFile():
m_pFSClient(NULL)
{
   m_GMP.init(0);
}

CCBFile::~CCBFile()
{
   m_GMP.close();
}

int CCBFile::open(const string& filename, const int& mode, char* cert)
{
   m_strFileName = filename;

   Node n;

   if (m_pFSClient->lookup(filename, &n) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(1); // locate file
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.m_iDataLength = 4 + filename.length() + 1;

   if (m_GMP.rpc(n.m_pcIP, CFSClient::m_iCBFSPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
   {
      int num = (msg.m_iDataLength - 4) / 68;

      cout << num << " copies found!" << endl;

      // choose closest server
      int c = 0;
      int rtt = 100000000;
      for (int i = 0; i < num; ++ i)
      {
         //cout << "RTT: " << msg.getData() + i * 68 << " " << *(int*)(msg.getData() + i * 68 + 64) << " " << m_GMP.rtt(msg.getData() + i * 68, *(int32_t*)(msg.getData() + i * 68 + 64)) << endl;
         int r = m_GMP.rtt(msg.getData() + i * 68, *(int32_t*)(msg.getData() + i * 68 + 64));
         if (r < rtt)
         {
            rtt = r;
            c = i;
         }
      }

      m_strServerIP = msg.getData() + c * 68;
      m_iServerPort = *(int32_t*)(msg.getData() + c * 68 + 64);
   }
   else
   {
      // file does not exist

      m_strServerIP = m_pFSClient->m_strServerHost;
      m_iServerPort = m_pFSClient->m_iServerPort;

      msg.setType(5); // create the file
      msg.setData(0, filename.c_str(), filename.length() + 1);
      msg.m_iDataLength = 4 + 64;

      if (m_GMP.rpc(m_strServerIP.c_str(), CFSClient::m_iCBFSPort, &msg, &msg) < 0)
         return -1;

      cout << "file owner certificate: " << msg.getData() << endl;
      if (NULL != cert)
         strcpy(cert, msg.getData());
   }

   msg.setType(2); // open the file
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.setData(64, (char*)&m_iProtocol, 4);
   msg.setData(68, (char*)&mode, 4);
   if (NULL != cert)
   {
      msg.setData(72, cert, strlen(cert) + 1);
      msg.m_iDataLength = 4 + 64 + 4 + 4 + strlen(cert) + 1;
   }
   else
      msg.m_iDataLength = 4 + 64 + 4 + 4;

   if (m_GMP.rpc(m_strServerIP.c_str(), CFSClient::m_iCBFSPort, &msg, &msg) < 0)
      return -1;

   if (1 == m_iProtocol)
      m_uSock = UDT::socket(AF_INET, SOCK_STREAM, 0);
   else
      m_tSock = socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = *(int*)(msg.getData()); // port
   inet_pton(AF_INET, m_strServerIP.c_str(), &serv_addr.sin_addr);
   memset(&(serv_addr.sin_zero), '\0', 8);

   if (1 == m_iProtocol)
   {
      if (UDT::ERROR == UDT::connect(m_uSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;

      //cout << "connect to UDT port " << *(int*)(msg.getData()) << endl;
   }
   else
   {
      if (-1 == connect(m_tSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;

      //cout << "connect to TCP port " << *(int*)(msg.getData()) << endl;
   }

   //cout << "connected!\n";

   return 1;
}

int CCBFile::read(char* buf, const int64_t& offset, const int64_t& size)
{
   int32_t cmd = 1;
   int64_t param[2];
   param[0] = offset;
   param[1] = size;
   int32_t response = -1;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (UDT::send(m_uSock, (char*)param, 8 * 2, 0) < 0)
         return -1;

      int h;
      if (UDT::recv(m_uSock, buf, size, 0, &h) < 0)
         return -1;
   }
   else
   {
      if (send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (send(m_tSock, (char*)param, 8 * 2, 0) < 0)
         return -1;

      int64_t rs = 0;
      while (rs < size)
      {
         int r = ::recv(m_tSock, buf, size, 0);
         if (r < 0)
            return -1;

         rs += r;
      }
   }

   return 1;
}

int CCBFile::write(const char* buf, const int64_t& offset, const int64_t& size)
{
   int32_t cmd = 2;
   int64_t param[2];
   param[0] = offset;
   param[1] = size;
   int32_t response = -1;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (UDT::send(m_uSock, (char*)param, 8 * 2, 0) < 0)
         return -1;

      int h;
      if (UDT::send(m_uSock, buf, size, 0, &h) < 0)
         return -1;
   }
   else
   {
      if (send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (send(m_tSock, (char*)param, 8 * 2, 0) < 0)
         return -1;

      int64_t ss = 0;
      while (ss < size)
      {
         int s = ::send(m_tSock, buf, size, 0);
         if (s < 0)
            return -1;

         ss += s;
      }
   }

   return 1;
}

int CCBFile::download(const char* localpath, const bool& cont)
{
   int32_t cmd = 3;
   int64_t offset;
   int64_t size;
   int32_t response = -1;

   ofstream ofs;

   if (cont)
   {
      ofs.open(localpath, ios::out | ios::binary | ios::app);
      ofs.seekp(0, ios::end);
      offset = ofs.tellp();
   }
   else
   {
      ofs.open(localpath, ios::out | ios::binary | ios::trunc);
      offset = 0LL;
   }

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (UDT::send(m_uSock, (char*)&offset, 8, 0) < 0)
         return -1;
      if (UDT::recv(m_uSock, (char*)&size, 8, 0) < 0)
         return -1;

      if (UDT::recvfile(m_uSock, ofs, offset, size) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (::send(m_tSock, (char*)&offset, 8, 0) < 0)
         return -1;
      if (::recv(m_tSock, (char*)&size, 8, 0) < 0)
         return -1;

      int64_t rs = 0;
      char buf[4096];
      while (rs < size)
      {
         int r = ::recv(m_tSock, buf, 4096, 0);
         if (r < 0)
            return -1;

         ofs.write(buf, r);

         rs += r;
      }
   }

   ofs.close();

   return 1;
}

int CCBFile::upload(const char* localpath, const bool& cont)
{
   int32_t cmd = 5;
   int64_t offset;
   int64_t size;
   int32_t response = -1;

   ifstream ifs;
   ifs.open(localpath, ios::in | ios::binary);

   ifs.seekg(0, ios::end);
   size = ifs.tellg();
   ifs.seekg(0);

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (UDT::send(m_uSock, (char*)&size, 8, 0) < 0)
         return -1;

      if (UDT::sendfile(m_uSock, ifs, 0, size) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;
      if ((recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (::send(m_tSock, (char*)&offset, 8, 0) < 0)
         return -1;
      if (::recv(m_tSock, (char*)&size, 8, 0) < 0)
         return -1;

      int64_t rs = 0;
      char buf[4096];
      while (rs < size)
      {
         int r = ::recv(m_tSock, buf, 4096, 0);
         if (r < 0)
            return -1;

         ifs.read(buf, r);

         rs += r;
      }
   }

   ifs.close();

   return 1;
}

int CCBFile::close()
{
   int32_t cmd = 4;

   if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
      return -1;

   UDT::close(m_uSock);

   return 1;
}

