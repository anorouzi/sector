#ifndef __SQL_CLIENT_H__
#define __SQL_CLIENT_H_

#include <routing.h>
#include <gmp.h>
#include <index.h>
#include <data.h>
#include <query.h>

class CSQLClient
{
friend class CQuery;

public:
   CSQLClient();
   CSQLClient(const int& protocol);
   ~CSQLClient();

public:
   int connect(const string& server, const int& port);
   int close();

public:
   CQuery* createQueryHandle();
   void releaseFileHandle(CQuery* q);

   int execute(const string& q);

private:
   int lookup(string table, Node* n);

private:
   string m_strServerHost;
   int m_iServerPort;

   CGMP* m_pGMP;

   int m_iProtocol;     // 1 UDT 2 TCP
};

class CQuery
{
public:
   CQuery();
   ~CQuery();

public:
   int open(const string& query);
   int close();

   int execute();   // return number of rows?
   int fetch(chat* res, int& rows, int& size);

private:
   CSQLClient* m_pSQLClient;

   string m_strServerIP;
   int m_iServerPort;

   CGMP m_GMP;

   string m_strQuery;
   CQueryAttr* m_pQuery;

   int m_iProtocol;     // 1 UDT 2 TCP

   UDTSOCKET m_uSock;
   int m_tSock;
};

#endif
