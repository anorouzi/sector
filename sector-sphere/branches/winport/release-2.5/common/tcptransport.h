/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __TCP_TRANSPORT_H__
#define __TCP_TRANSPORT_H__

#include <string>

#ifdef WIN32
    #ifdef COMMON_EXPORTS
        #define COMMON_API __declspec(dllexport)
    #else
        #define COMMON_API __declspec(dllimport)
    #endif
#else
    #define COMMON_API
#endif

class COMMON_API TCPTransport
{
public:
   TCPTransport();
   ~TCPTransport();

public:
   int open(const char* ip, const int& port);
   int listen();
   TCPTransport* accept(char* ip, int& port);
   int connect(const char* ip, const int& port);
   int rendezvous_connect(const char* ip, const int& port, bool cs = false) {return 0;}
   int close();

   int send(const char* data, const int& size);
   int recv(char* data, const int& size);

   int64_t sendfile(const char* file, const int64_t& offset, const int64_t& size);
   int64_t recvfile(const char* file, const int64_t& offset, const int64_t& size);

   int getLocalIP(std::string& ip);

private:
#ifndef WIN32
   int m_iSocket;
#else
   SOCKET m_iSocket;
#endif
   bool m_bConnected;
};

#endif
