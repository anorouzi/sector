#ifndef __CB_CLIENT_H__
#define __CB_CLIENT_H__

#include <gmp.h>
#include <node.h>

class Client
{
public:
   Client();
   Client(const int& protocol);
   ~Client();

public:
   int connect(const string& server, const int& port);
   int close();

protected:
   int lookup(string filename, Node* n);

protected:
   string m_strServerHost;
   int m_iServerPort;

   CGMP* m_pGMP;

   int m_iProtocol;     // 1 UDT 2 TCP
};

#endif
