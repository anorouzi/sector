#include <transport.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <fstream>

using namespace std;


CTCPTransport::~CTCPTransport()
{
}

int CTCPTransport::listen(const int& port)
{
   m_iSocket = socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = port;
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);

   if (::bind(m_iSocket, (sockaddr*)&my_addr, sizeof(my_addr)) < 0)
   {
      ::close(m_iSocket);
      return -1;
   }

   ::listen(m_iSocket, 1);

   int size = sizeof(sockaddr_in);
   ::getsockname(m_iSocket, (sockaddr*)&my_addr, (socklen_t*)&size);

   return my_addr.sin_port;
}

int CTCPTransport::accept()
{
   int t = m_iSocket;
   m_iSocket = ::accept(t, NULL, NULL);
   ::close(t);

   return m_iSocket;
}

int CTCPTransport::connect(const char* ip, const int& port)
{
   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = port;
   inet_pton(AF_INET, ip, &serv_addr.sin_addr);
   memset(&(serv_addr.sin_zero), '\0', 8);

   if (-1 == ::connect(m_iSocket, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   return 1;
}

int CTCPTransport::send(char* data, const int& size)
{
   int ss = 0;
   while (ss < size)
   {
      int s = ::send(m_iSocket, data + ss, size - ss, 0);
      if (s < 0)
         return -1;

      ss += s;
   }

   return ss;
}

int CTCPTransport::recv(char* data, const int& size)
{
   int rs = 0;
   while (rs < size)
   {
      int r = ::recv(m_iSocket, data + rs, size - rs, 0);
      if (r < 0)
         return -1;

      rs += r;
   }

   return rs;
}

int CTCPTransport::sendfile(const char* filename, const int64_t& offset, const int64_t& size)
{
   int fd = ::open(filename, O_RDONLY);

   if (::sendfile(m_iSocket, fd, (off_t*)&offset, size) < 0)
   {
      ::close(fd);
      return -1;
   }

   ::close(fd);
   return size;
}

int CTCPTransport::recvfile(const char* filename, const int64_t& offset, const int64_t& size)
{
   char* temp = new char[size];

   int rs = 0;
   while (rs < size)
   {
      int r = ::recv(m_iSocket, temp + rs, size - rs, 0);
      if (r < 0)
         return -1;

      rs += r;
   }

   ofstream ofs(filename);
   ofs.seekp(offset, ios::beg);
   ofs.write(temp, rs);
   ofs.close();

   delete [] temp;

   return size;
}


CUDTTransport::~CUDTTransport()
{

}

int CUDTTransport::listen(const int& port)
{
   m_iSocket = socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = port;
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);

   if (UDT::bind(m_iSocket, (sockaddr*)&my_addr, sizeof(my_addr)) < 0)
   {
      UDT::close(m_iSocket);
      return -1;
   }

   UDT::listen(m_iSocket, 1);

   int size = sizeof(sockaddr_in);
   UDT::getsockname(m_iSocket, (sockaddr*)&my_addr, &size);

   return my_addr.sin_port;
}

int CUDTTransport::accept()
{
   UDTSOCKET u = m_iSocket;
   m_iSocket = UDT::accept(u, NULL, NULL);
   UDT::close(u);

   return m_iSocket;
}

int CUDTTransport::connect(const char* ip, const int& port)
{
   m_iSocket = UDT::socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = port; // port
   inet_pton(AF_INET, ip, &serv_addr.sin_addr);
   memset(&(serv_addr.sin_zero), '\0', 8);

   if (UDT::ERROR == UDT::connect(m_iSocket, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   return 1;
}

int CUDTTransport::send(char* data, const int& size)
{
   int h;
   return UDT::send(m_iSocket, data, size, 0, &h);
}

int CUDTTransport::recv(char* data, const int& size)
{
   int h;
   return UDT::recv(m_iSocket, data, size, 0, &h);
}

int CUDTTransport::sendfile(const char* filename, const int64_t& offset, const int64_t& size)
{
   ifstream ifs;

   ifs.open(filename, ios::in | ios::binary);

   int res = UDT::sendfile(m_iSocket, ifs, offset, (int64_t&)size);

   ifs.close();

   return res;
}

int CUDTTransport::recvfile(const char* filename, const int64_t& offset, const int64_t& size)
{
   ofstream ofs;

   ofs.open(filename, ios::out | ios::binary);

   int res = UDT::recvfile(m_iSocket, ofs, offset, (int64_t&)size);

   ofs.close();

   return res;
}
