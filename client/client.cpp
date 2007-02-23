#include "client.h"


Client::Client():
m_iProtocol(1)
{
   m_pGMP = new CGMP;
}

Client::Client(const int& protocol):
m_iProtocol(protocol)
{
   m_pGMP = new CGMP;
}

Client::~Client()
{
   delete m_pGMP;
}

int Client::connect(const string& server, const int& port)
{
   m_strServerHost = server;
   m_iServerPort = port;

   m_pGMP->init(0);

   return 1;
}

int Client::close()
{
   m_pGMP->close();

   return 1;
}

int Client::lookup(string name, Node* n)
{
   CCBMsg msg;
   msg.setType(4); // look up a file server
   msg.setData(0, name.c_str(), name.length() + 1);
   msg.m_iDataLength = 4 + name.length() + 1;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      memcpy(n, msg.getData(), sizeof(Node));

   return msg.getType();
}
