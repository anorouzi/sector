#include <sqlclient.h>
#include <iostream>

using namespace std;

Query* SQLClient::createQueryHandle()
{
   Query *q = NULL;

   try
   {
      q = new Query;
   }
   catch (...)
   {
      return NULL;
   }

   q->m_pSQLClient = this;
   q->m_iProtocol = m_iProtocol;

   return q;
}

void SQLClient::releaseQueryHandle(Query* q)
{
   delete q;
}

int SQLClient::getSemantics(const string& name, vector<DataAttr>& attr)
{
   CCBMsg msg;
   msg.setType(201); // semantics
   msg.setData(0, name.c_str(), name.length() + 1);
   msg.m_iDataLength = 4 + name.length() + 1;

cout << m_strServerHost.c_str() << " " << m_iServerPort << endl;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

cout << "got response\n";

   if (msg.getType() > 0)
      Semantics::deserialize(msg.getData(), attr);

   return msg.getType();
}

Query::Query():
m_pSQLClient(NULL)
{
   m_GMP.init(0);
}

Query::~Query()
{
   m_GMP.close();
}

int Query::open(const string& query)
{
   if (0 != SQLParser::parse(query, m_SQLExpr))
      return -1;

   // currently we can only deal with single table
   if (m_SQLExpr.m_vstrTableList.size() != 1)
      return 1;

   string table = m_SQLExpr.m_vstrTableList[0];

   Node n;
   if (m_pSQLClient->lookup(table, &n) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(1); // locate file
   msg.setData(0, table.c_str(), table.length() + 1);
   msg.m_iDataLength = 4 + table.length() + 1;

   if (m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg) < 0)
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
      // SQL client is read only.
      return -1;
   }

   msg.setType(200); // open the file
   msg.setData(0, table.c_str(), table.length() + 1);
   msg.setData(64, (char*)&m_iProtocol, 4);
   msg.setData(68, query.c_str(), query.length() + 1);
   msg.m_iDataLength = 4 + 64 + 4 + query.length() + 1;

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (1 == m_iProtocol)
   {
      m_uSock = UDT::socket(AF_INET, SOCK_STREAM, 0);

      #ifdef WIN32
         int mtu = 1052;
         UDT::setsockopt(m_uSock, 0, UDT_MSS, &mtu, sizeof(int));
      #endif
   }
   else
      m_tSock = ::socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = *(int*)(msg.getData()); // port
   #ifndef WIN32
      inet_pton(AF_INET, m_strServerIP.c_str(), &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(m_strServerIP.c_str());
   #endif
      memset(&(serv_addr.sin_zero), '\0', 8);

   if (1 == m_iProtocol)
   {
      if (UDT::ERROR == UDT::connect(m_uSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;
   }
   else
   {
      if (-1 == ::connect(m_tSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;
   }

   m_strQuery = query;

   return 0;
}

int Query::close()
{
   int32_t cmd = 2; // close

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;

      UDT::close(m_uSock);
   }
   else
   {
      if (::send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;

      closesocket(m_tSock);
   }

   return 1;
}

int Query::fetch(char* res, int& rows, int& size)
{
   char req[8];
   *(int32_t*)req = 1; // fetch (more) records
   *(int32_t*)(req + 4) = rows;
   int32_t response[2];
   response[0] = -1;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, req, 8, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)response, 8, 0) < 0) || (-1 == response[0]))
         return -1;

      size = response[1];
      int h;
      if (UDT::recv(m_uSock, res, size, 0, &h) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, req, 8, 0) < 0)
         return -1;
      if ((::recv(m_tSock, (char*)response, 8, 0) < 0) || (-1 == response[0]))
         return -1;

      size = response[1];
      int64_t rs = 0;
      while (rs < size)
      {
         int r = ::recv(m_tSock, res, size, 0);
         if (r < 0)
            return -1;

         rs += r;
      }
   }

   return size;
}
