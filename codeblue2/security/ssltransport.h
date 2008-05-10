/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 04/11/2008
*****************************************************************************/

#ifndef __SSL_TRANSPORT_H__
#define __SSL_TRANSPORT_H__

#include "openssl/bio.h"
#include "openssl/ssl.h"
#include "openssl/err.h"

class SSLTransport
{
public:
   SSLTransport();
   ~SSLTransport();

public:
   static void init();
   static void destroy();

public:
   int initServerCTX(const char* cert, const char* key);
   int initClientCTX(const char* cert);

   int open(const char* ip, const int& port);
   int listen();
   SSLTransport* accept(char* ip, int& port);
   int connect(const char* ip, const int& port);
   int close();

   int send(const char* data, const int& size);
   int recv(char* data, const int& size);

private:
   SSL_CTX* m_pCTX;
   SSL* m_pSSL;
   BIO* m_pBIO;
   int m_iSocket;

private:
   static int s_iInstance;
};

#endif
