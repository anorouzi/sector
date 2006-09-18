#ifndef __TRANSPORT_H__
#define __TRNASPORT_H__

#include <udt.h>

enum PROTOCOL {TCP_T, UDT_T};

class CTransport
{
public:
   virtual ~CTransport() {}

public:
   virtual int listen(const int& port = 0) = 0;
   virtual int accept() = 0;
   virtual int connect(const char* ip, const int& port) = 0;

public:
   virtual int send(char* data, const int& size) = 0;
   virtual int recv(char* data, const int& size) = 0;
   virtual int sendfile(const char* filename, const int64_t& offset, const int64_t& size) = 0;
   virtual int recvfile(const char* filename, const int64_t& offset, const int64_t& size) = 0;

private:

};

class CTCPTransport: public CTransport
{
public:
   virtual ~CTCPTransport();

public:
   virtual int listen(const int& port = 0);
   virtual int accept();
   virtual int connect(const char* ip, const int& port);

public:
   virtual int send(char* data, const int& size);
   virtual int recv(char* data, const int& size);
   virtual int sendfile(const char* filename, const int64_t& offset, const int64_t& size);
   virtual int recvfile(const char* filename, const int64_t& offset, const int64_t& size);

public:
   int m_iSocket;
};

class CUDTTransport: public CTransport
{
public:
   virtual ~CUDTTransport();

public:
   virtual int listen(const int& port = 0);
   virtual int accept();
   virtual int connect(const char* ip, const int& port);

public:
   virtual int send(char* data, const int& size);
   virtual int recv(char* data, const int& size);
   virtual int sendfile(const char* filename, const int64_t& offset, const int64_t& size);
   virtual int recvfile(const char* filename, const int64_t& offset, const int64_t& size);

public:
   UDTSOCKET m_iSocket;
};

#endif
