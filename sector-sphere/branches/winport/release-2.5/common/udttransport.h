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


#ifndef __UDT_TRANSPORT_H__
#define __UDT_TRANSPORT_H__

#include "udt.h"
#include "crypto.h"

class COMMON_API UDTTransport
{
public:
   UDTTransport();
   ~UDTTransport();

public:
   static void initialize();
   static void release();

   int open(int& port, bool rendezvous = true, bool reuseaddr = false);

   int listen();
   int accept(UDTTransport& t, sockaddr* addr = NULL, int* addrlen = NULL);

   int connect(const char* ip, int port);
   int send(const char* buf, int size);
   int recv(char* buf, int size);
   int64_t sendfile(std::fstream& ifs, int64_t offset, int64_t size);
   int64_t recvfile(std::fstream& ofs, int64_t offset, int64_t size);
   int close();
   bool isConnected();
   int64_t getRealSndSpeed();
   int getsockname(sockaddr* addr);

public: // secure data/file transfer
   int initCoder(unsigned char key[16], unsigned char iv[8]);
   int releaseCoder();

   int secure_send(const char* buf, int size);
   int secure_recv(char* buf, int size);
   int64_t secure_sendfile(std::fstream& ifs, int64_t offset, int64_t size);
   int64_t secure_recvfile(std::fstream& ofs, int64_t offset, int64_t size);

public:
   int sendEx(const char* buf, int size, bool secure);
   int recvEx(char* buf, int size, bool secure);
   int64_t sendfileEx(std::fstream& ifs, int64_t offset, int64_t size, bool secure);
   int64_t recvfileEx(std::fstream& ofs, int64_t offset, int64_t size, bool secure);

private:
   UDTSOCKET m_Socket;

   Crypto m_Encoder;
   Crypto m_Decoder;
};

#endif
