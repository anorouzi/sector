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
   Yunhong Gu [gu@lac.uic.edu], last updated 08/01/2008
*****************************************************************************/

#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "ssltransport.h"

#include <iostream>
using namespace std;

int SSLTransport::s_iInstance = 0;

SSLTransport::SSLTransport():
m_pCTX(NULL),
m_pSSL(NULL),
m_pBIO(NULL),
m_iSocket(0)
{
}

SSLTransport::~SSLTransport()
{
}

void SSLTransport::init()
{
   if (0 == s_iInstance)
   {
      SSL_load_error_strings();
      ERR_load_BIO_strings();
      ERR_load_SSL_strings();
      SSL_library_init();
   }

   s_iInstance ++;
}

void SSLTransport::destroy()
{
   s_iInstance --;
}

int SSLTransport::initServerCTX(const char* cert, const char* key)
{
   m_pCTX = SSL_CTX_new(SSLv23_server_method());
   if (m_pCTX == NULL)
   {
      cerr << "Failed init CTX. Aborting.\n";
      return -1;
   }

   if (!SSL_CTX_use_certificate_file(m_pCTX, cert, SSL_FILETYPE_PEM) || !SSL_CTX_use_PrivateKey_file(m_pCTX, key, SSL_FILETYPE_PEM))
   {
      ERR_print_errors_fp(stderr);
      SSL_CTX_free(m_pCTX);
      return -1;
   }

   return 1;
}

int SSLTransport::initClientCTX(const char* cert)
{
   m_pCTX = SSL_CTX_new(SSLv23_client_method());

   if(!SSL_CTX_load_verify_locations(m_pCTX, cert, NULL))
   {
      cerr << "Error loading trust store: " << cert << endl;
      SSL_CTX_free(m_pCTX);
      return -1;
   }

   return 1;
}

int SSLTransport::open(const char* ip, const int& port)
{
   if ((m_iSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
      return -1;

   if ((NULL == ip) && (0 == port))
      return 0;

   sockaddr_in addr;
   memset(&addr, 0, sizeof(sockaddr_in));
   addr.sin_addr.s_addr = INADDR_ANY;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);

   int reuse = 1;
   ::setsockopt(m_iSocket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

   if (::bind(m_iSocket, (sockaddr*)&addr, sizeof(sockaddr_in)) < 0)
   {
      cerr << "SSL socket unable to bind on address " << ip << " " << port << endl;
      return -1;
   }

   return 0;
}

int SSLTransport::listen()
{
   return ::listen(m_iSocket, 10);
}

SSLTransport* SSLTransport::accept(char* ip, int& port)
{
   SSLTransport* t = new SSLTransport;

   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);
   if ((t->m_iSocket = ::accept(m_iSocket, (sockaddr*)&addr, &size)) < 0)
      return NULL;

   inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
   port = addr.sin_port;

   t->m_pBIO = BIO_new_socket(t->m_iSocket, BIO_NOCLOSE);
   t->m_pSSL = SSL_new(m_pCTX);
   SSL_set_bio(t->m_pSSL, t->m_pBIO, t->m_pBIO);

   if (SSL_accept(t->m_pSSL) <= 0)
      return NULL;

   return t;
}

int SSLTransport::connect(const char* host, const int& port)
{
   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   hostent* he = gethostbyname(host);

   if (NULL == he)
   {
      cerr << "SSL connect: invalid address " << host << " " << port << endl;
      return -1;
   }

   addr.sin_addr.s_addr = ((in_addr*)he->h_addr)->s_addr;
   memset(addr.sin_zero, '\0', sizeof(addr.sin_zero));

   if (::connect(m_iSocket, (sockaddr*)&addr, sizeof(sockaddr_in)) < 0)
   {
      cerr << "SSL connect: unable to connect to server.\n";
      return -1;
   }

   m_pSSL = SSL_new(m_pCTX);
   m_pBIO = BIO_new_socket(m_iSocket, BIO_NOCLOSE);
   SSL_set_bio(m_pSSL, m_pBIO, m_pBIO);

   if (SSL_connect(m_pSSL) <= 0)
      return -1;

   if (SSL_get_verify_result(m_pSSL) != X509_V_OK)
   {
      cerr << "failed verify SSL certificate.\n";
      return -1;
   }

   X509* peer = SSL_get_peer_certificate(m_pSSL);
   char peer_CN[256];
   X509_NAME_get_text_by_NID(X509_get_subject_name(peer), NID_commonName, peer_CN, 256);
   //if (strcasecmp(peer_CN, host))
   //{
   //   cerr << "server name does not match.\n";
   //   return -1;
   //}

   return 1;
}

int SSLTransport::close()
{
   return ::close(m_iSocket);
}

int SSLTransport::send(const char* data, const int& size)
{
   int ts = size;
   while (ts > 0)
   {
      int s = SSL_write(m_pSSL, data + size - ts, ts);
      if (s <= 0)
         return -1;
      ts -= s;
   }
   return size;
}

int SSLTransport::recv(char* data, const int& size)
{
   int tr = size;
   while (tr > 0)
   {
      int r = SSL_read(m_pSSL, data + size - tr, tr);
      if (r <= 0)
         return -1;
      tr -= r;
   }
   return size;
}

int SSLTransport::getLocalIP(string& ip)
{
   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);

   if (getsockname(m_iSocket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char tmp[64];

   ip = inet_ntop(AF_INET, &(addr.sin_addr), tmp, 64);

   return 1;
}
