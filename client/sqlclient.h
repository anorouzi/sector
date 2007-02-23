#ifndef __SQL_CLIENT_H__
#define __SQL_CLIENT_H_

#include <gmp.h>
#include <index.h>
#include <data.h>
#include <sql.h>
#include <udt.h>
#include <client.h>

class SQLClient: public Client
{
friend class Query;

public:
   Query* createQueryHandle();
   void releaseQueryHandle(Query* q);

   int getSemantics(const string& name, vector<DataAttr>& attr);
};

class Query
{
friend class SQLClient;

public:
   Query();
   ~Query();

public:
   int open(const string& query);
   int close();

   int fetch(char* res, int& rows, int& size);

private:
   SQLClient* m_pSQLClient;

   string m_strServerIP;
   int m_iServerPort;

   CGMP m_GMP;

   string m_strQuery;
   SQLExpr m_SQLExpr;

   int m_iProtocol;     // 1 UDT 2 TCP

   UDTSOCKET m_uSock;
   int m_tSock;
};

#endif
