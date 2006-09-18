#include <sqlclient.h>
#include <iostream>

using namespace std;

CSQLClient::CSQLClient():
m_iProtocol(1)
{
   m_pGMP = new CGMP;
}

CSQLClient::CSQLClient(const int& protocol):
m_iProtocol(protocol)
{
   m_pGMP = new CGMP;
}

CSQLClient::~CSQLClient()
{
   delete m_pGMP;
}

int CSQLClient::connect(const string& server, const int& port)
{
   m_strServerHost = server;
   m_iServerPort = port;

   m_pGMP->init(0);

   return 1;
}

int CSQLClient::close()
{
   m_pGMP->close();

   return 1;
}

CQuery* CSQLClient::createQueryHandle()
{
   CQuery *q = NULL;

   try
   {
      f = new CQuery;
   }
   catch (...)
   {
      return NULL;
   }

   q->m_pSQLClient = this;
   q->m_iProtocol = m_iProtocol;

   return q;
}

void CSQLClient::releaseQueryHandle(CQuery* q)
{
   delete q;
}


CQuery::CQuery():
m_pSQLClient(NULL),
m_pQuery(NULL)
{
   m_GMP.init(0);
}

CQuery::~CQuery()
{
   m_GMP.close();
}

int CQuery::open(const string& query)
{
   m_pQuery = CParser::parse(query.c_str(), query.length());

   if (NULL == m_pQuery)
      return -1;
}

int CQuery::close()
{
   delete m_pQuery;
}

int CQuery::execute()
{
   
}

int CQuery::fetch(chat* res, int& rows, int& size)
{

}
