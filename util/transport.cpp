#include <transport.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace std;
using namespace cb;

Transport::Transport()
{
}

Transport::~Transport()
{
}

int Transport::open(int& port)
{
   m_Socket = UDT::socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = 0;
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);

   UDT::bind(m_Socket, (sockaddr*)&my_addr, sizeof(my_addr));
 
   int size = sizeof(sockaddr_in);
   UDT::getsockname(m_Socket, (sockaddr*)&my_addr, &size);
   port = my_addr.sin_port;

   #ifdef WIN32
      int mtu = 1052;
      UDT::setsockopt(m_Socket, 0, UDT_MSS, &mtu, sizeof(int));
   #endif

   bool rendezvous = 1;
   UDT::setsockopt(m_Socket, 0, UDT_RENDEZVOUS, &rendezvous, sizeof(bool));

   return 1;
}

int Transport::connect(const char* ip, const int& port)
{
   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = port;
   #ifndef WIN32
      inet_pton(AF_INET, ip, &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(ip);
   #endif
      memset(&(serv_addr.sin_zero), '\0', 8);

   if (UDT::ERROR == UDT::connect(m_Socket, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   return 1;
}

int Transport::send(const char* buf, const int& size)
{
   int ssize = 0;
   while (ssize < size)
   {
      int ss = UDT::send(m_Socket, buf + ssize, size - ssize, 0);
      if (UDT::ERROR == ss)
         return -1;

      ssize += ss;
   }

   return ssize;
}

int Transport::recv(char* buf, const int& size)
{
   int rsize = 0;
   while (rsize < size)
   {
      int rs = UDT::recv(m_Socket, buf + rsize, size - rsize, 0);
      if (UDT::ERROR == rs)
         return -1;

      rsize += rs;
   }

   return rsize;
}

int Transport::sendfile(ifstream& ifs, const int64_t& offset, const int64_t& size)
{
   return UDT::sendfile(m_Socket, ifs, offset, size);
}

int Transport::recvfile(ofstream& ifs, const int64_t& offset, const int64_t& size)
{
   return UDT::recvfile(m_Socket, ifs, offset, size);
}

int Transport::close()
{
   return UDT::close(m_Socket);
}
