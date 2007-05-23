#ifndef __CB_TRANSPORT_H__
#define __CB_TRANSPORT_H__

#include <udt.h>

namespace cb
{

class Transport
{
public:
   Transport();
   ~Transport();

public:
   int open(int& port);
   int connect(const char* ip, const int& port);
   int send(const char* buf, const int& size);
   int recv(char* buf, const int& size);
   int sendfile(std::ifstream& ifs, const int64_t& offset, const int64_t& size);
   int recvfile(std::ofstream& ifs, const int64_t& offset, const int64_t& size);
   int close();

private:
   UDTSOCKET m_Socket;
};

};

#endif
