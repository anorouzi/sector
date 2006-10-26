#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/sendfile.h>
#include <store.h>
#include <assert.h>
#include <sstream>
#include <signal.h>

using namespace std;


const int CStore::m_iCBFSPort = 2237; //cbfs


CStore::CStore(const string& ip)
{
   m_strLocalHost = ip;
   m_iLocalPort = m_iCBFSPort;

   m_GMP.init(m_iLocalPort);
}

CStore::~CStore()
{
   m_GMP.close();
}

int CStore::init(char* ip, int port)
{
   int res;

   if (NULL == ip)
   {
      res = m_Router.start(m_strLocalHost.c_str());
   }
   else
   {
      ///////////////////////////////
      // We use a fixed port here. should be update later
      res = m_Router.join(m_strLocalHost.c_str(), ip, CRouting::m_iRouterPort);
   }

   if (res < 0)
      return -1;

   if (initLocalFile() < 0)
      return -1;

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, run, this);
   pthread_detach(msgserver);

   // ignore TCP broken pipe
   signal(SIGPIPE, SIG_IGN);

   return 1;
}

int CStore::run()
{
   int next = 0;

   while (true)
   {
      updateOutLink();
      sleep(10);

      updateInLink();
      sleep(10);

      updateNameIndex(next);
      sleep(10);

      // check out link more often since it is more important
      updateOutLink();
      sleep(10);
   }

   return 1;
}

void* CStore::run(void* s)
{
   CStore* self = (CStore*)s;

   char ip[64];
   int port;
   int32_t id;
   CCBMsg* msg = new CCBMsg;
   msg->resize(65536);

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_sec);

   while (true)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      //cout << "recv CB " << msg->getType() << " " << ip << " " << port << endl;

      switch (msg->getType())
      {
      case 1: // locate file
         {
            string filename = msg->getData();

            set<CFileAttr, CAttrComp> filelist;

            if (self->m_RemoteFile.lookup(filename, &filelist) < 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
            }
            else
            {
               /*
               int r = (int)(filelist.size() * rand() / double(RAND_MAX));
               set<CFileAttr, CAttrComp>::iterator i = filelist.begin();
               for (int j = 0; j < r; ++ j)
                  ++ i;

               msg->setData(0, i->m_pcHost, strlen(i->m_pcHost) + 1);
               msg->setData(64, (char*)(&i->m_iPort), 4);
               msg->m_iDataLength = 4 + 64 + 4;

               cout << "locate " << filename << " " << filelist.size() << " " << i->m_pcHost << " " << i->m_iPort << endl;
               */

               // feedback all copies of the requested file
               int num = 0;
               for (set<CFileAttr, CAttrComp>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
               {
                  msg->setData(num * 68, i->m_pcHost, strlen(i->m_pcHost) + 1);
                  msg->setData(num * 68 + 64, (char*)(&i->m_iPort), 4);
                  ++ num;
               }
               msg->m_iDataLength = 4 + num * (64 + 4);

               cout << "locate " << filename << ": " << filelist.size() << " found!" << endl;
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 2: // open file
         {
            char* filename = msg->getData();

            int conn = *(int*)(msg->getData() + 64);
            int mode = *(int*)(msg->getData() + 68);
            char cert[1024];
            if (msg->m_iDataLength > 4 + 64 + 4 + 4)
               strcpy(cert, msg->getData() + 72);
            else
               cert[0] = '\0';

            set<CFileAttr, CAttrComp> filelist;
            if (self->m_LocalFile.lookup(filename, &filelist) < 0)
            {
               CFileAttr attr;
               memcpy(attr.m_pcName, filename, 64);
               attr.m_iAttr = 3;
               attr.m_llSize = 0;
               memcpy(attr.m_pcHost, self->m_strLocalHost.c_str(), 64);
               attr.m_iPort = self->m_iLocalPort;
               self->m_LocalFile.insert(attr);
               filelist.insert(attr);;
            }

            // set IO attributes: READ WRITE
            // mode &= attr.m_iType;
            // TO DO: check ownership and previlege;
            ifstream ifs;
            ifs.open((self->m_strHomeDir + "/.cert/" + filename + ".cert").c_str());
            char ecert[1024];
            ecert[0] = '\0';
            ifs.getline(ecert, 1024);

            if ((0 == strlen(cert)) || (0 == strlen(ecert)))
               mode = 0;
            else
            {
               unsigned char sha[SHA_DIGEST_LENGTH + 1];
               SHA1((const unsigned char*)cert, strlen(cert), sha);
               sha[SHA_DIGEST_LENGTH] = '\0';
               stringstream shastr(stringstream::in | stringstream::out);
               for (int i = 0; i < SHA_DIGEST_LENGTH; i += 4)
                  shastr << *(int32_t*)(sha + i);

               if (shastr.str() == string(ecert))
                  mode = 1;
               else
                  mode = 0;
            }

            self->m_AccessLog.insert(ip, port, msg->getData());

            cout << "===> start file server " << endl;

            UDTSOCKET u;
            int t;

            if (1 == conn)
               u = UDT::socket(AF_INET, SOCK_STREAM, 0);
            else
               t = socket(AF_INET, SOCK_STREAM, 0);

            sockaddr_in my_addr;
            my_addr.sin_family = AF_INET;
            my_addr.sin_port = 0;
            my_addr.sin_addr.s_addr = INADDR_ANY;
            memset(&(my_addr.sin_zero), '\0', 8);

            int res;
            if (1 == conn)
               res = UDT::bind(u, (sockaddr*)&my_addr, sizeof(my_addr));
            else
               res = bind(t, (sockaddr*)&my_addr, sizeof(my_addr));

            if (((1 == conn) && (UDT::ERROR == res)) || ((2 == conn) && (-1 == res)))
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            if (1 == conn)
               UDT::listen(u, 1);
            else
               listen(t, 1);

            Param2* p = new Param2;
            p->s = self;
            p->fn = msg->getData();
            p->u = u;
            p->t = t;
            p->c = conn;
            p->m = mode;

            pthread_t remote_thread;
            pthread_create(&remote_thread, NULL, remote, p);
            pthread_detach(remote_thread);

            int size = sizeof(sockaddr_in);
            if (1 == conn)
               UDT::getsockname(u, (sockaddr*)&my_addr, &size);
            else
               getsockname(t, (sockaddr*)&my_addr, (socklen_t*)&size);

            msg->setData(0, (char*)&my_addr.sin_port, 4);
            msg->m_iDataLength = 4 + 4;
            //cout << "feedback port " << my_addr.sin_port <<endl;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
        }

      case 3: // add a new file
         {
            CFileAttr attr;
            attr.desynchronize(msg->getData(), msg->m_iDataLength - 4);

            //cout << "remote file : " << attr.m_pcName << " " << attr.m_llSize << endl;

            if (self->m_RemoteFile.insert(attr) < 0)
               msg->setType(-msg->getType());

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 4: // lookup a file server
         {
            string filename = msg->getData();
            int fid = CDHash::hash(filename.c_str(), m_iKeySpace);

            int r = self->m_Router.lookup(fid, (Node*)msg->getData());

            if (-1 == r)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
            }
            else
            {
               msg->m_iDataLength = 4 + sizeof(Node);
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 5: // create a local file
         {
            string filename = msg->getData();

            if (self->m_LocalFile.lookup(filename) > 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            string file = self->m_strHomeDir + filename;

            ofstream fs;
            fs.open(file.c_str());
            fs.close();

            CFileAttr attr;
            strcpy(attr.m_pcName, filename.c_str());
            strcpy(attr.m_pcHost, self->m_strLocalHost.c_str());
            attr.m_iPort = self->m_iLocalPort;

            self->m_LocalFile.insert(attr);

            // generate certificate for the file owner
            char cert[1024];
            timeval t;
            gettimeofday(&t, 0);
            srand(t.tv_sec);
            sprintf(cert, "%s %d %s %d%d%d%d%d", ip, port, filename.c_str(), rand(), rand(), rand(), rand(), rand());

            unsigned char sha[SHA_DIGEST_LENGTH + 1];
            SHA1((const unsigned char*)cert, strlen(cert), sha);
            sha[SHA_DIGEST_LENGTH] = '\0';

            DIR* test = opendir((self->m_strHomeDir + ".cert").c_str());

            if (NULL == test)
            {
               if ((errno != ENOENT) || (mkdir((self->m_strHomeDir + ".cert").c_str(), S_IRWXU) < 0))
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;

                  self->m_GMP.sendto(ip, port, id, msg);
                  break;
               }
            }
            closedir(test);

            ofstream cf((self->m_strHomeDir + ".cert/" + filename + ".cert").c_str());
            for (int i = 0; i < SHA_DIGEST_LENGTH; i += 4)
               cf << *(int32_t*)(sha + i);
            cf.close();

            msg->setData(0, cert, strlen(cert) + 1);
            msg->m_iDataLength = 4 + strlen(cert) + 1;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 6: // probe the existence of a file
         {
            string filename = msg->getData();

            if (self->m_LocalFile.lookup(filename) <= 0)
               msg->setType(-msg->getType());

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 7: // update name index
         {
            for (int i = 0; i < (msg->m_iDataLength - 4) / 64; ++ i)
            {
               self->m_NameIndex.insert(msg->getData() + i * 64, ip, m_iCBFSPort);
            }

            //cout << "global name index updated " << (msg->m_iDataLength - 4) / 64 << endl;

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 10: // remove file from RemoteFileIndex
         {
            self->m_RemoteFile.remove(msg->getData());
            msg->m_iDataLength = 4;

            break;
         }

      default:
         {
            Param1* p = new Param1;
            p->s = self;
            memcpy(p->ip, ip, 64);
            p->id = id;
            p->port = port;
            p->msg = new CCBMsg(*msg);

            pthread_t process_thread;
            pthread_create(&process_thread, NULL, process, p);
            pthread_detach(process_thread);

            break;
         }
      }

      //cout << "respond CB " << msg->getType() << endl;
   }

   delete msg;
   return NULL;
}

void* CStore::process(void* p)
{
   CStore* self = ((Param1*)p)->s;
   char* ip = ((Param1*)p)->ip;
   int port = ((Param1*)p)->port;
   int32_t id = ((Param1*)p)->id;
   CCBMsg* msg = ((Param1*)p)->msg;

   //cout << "recv request " << msg->getType() << endl;

   switch (msg->getType())
   {
   case 8: // retrieve name index
      {
         if (*(int32_t*)(msg->getData()) == 1)
         {
            vector<string> filelist;
            self->m_NameIndex.search(filelist);
            CNameIndex::synchronize(filelist, msg->getData(), msg->m_iDataLength);
            msg->m_iDataLength += 4;

            break;
         }

         int loc = (int)(m_iKeySpace * rand() / (RAND_MAX+1.0));
         Node n;

         self->m_Router.lookup(loc, &n);

         if (string(n.m_pcIP) == self->m_strLocalHost)
         {
            vector<string> filelist;
            self->m_NameIndex.search(filelist);
            CNameIndex::synchronize(filelist, msg->getData(), msg->m_iDataLength);
            msg->m_iDataLength += 4;
         }
         else
         {
            *(int32_t*)(msg->getData()) = 1;
            msg->m_iDataLength = 4 + 4;

            if (self->m_GMP.rpc(n.m_pcIP, m_iCBFSPort, msg, msg) < 0)
               msg->setType(-msg->getType());
         }

         break;
      }

   case 9: // stat
      {
         string filename = msg->getData();
         int fid = CDHash::hash(filename.c_str(), m_iKeySpace);
         Node n;

         int r = self->m_Router.lookup(fid, &n);

         if (-1 == r)
         {
            msg->setType(-msg->getType());
            msg->m_iDataLength = 4;
         }
         else
         {
            if (self->m_strLocalHost == n.m_pcIP)
            {
               set<CFileAttr, CAttrComp> sa;
               if (self->m_RemoteFile.lookup(filename, &sa) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
               else
               {
                  sa.begin()->synchronize(msg->getData(), msg->m_iDataLength);
                  msg->m_iDataLength += 4;
               }

               cout << "syn " << filename << " " << msg->getType() << " " << msg->m_iDataLength - 4 << " " << sa.begin()->m_llSize << endl;
            }
            else
            {
               if (self->m_GMP.rpc(n.m_pcIP, m_iCBFSPort, msg, msg) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
            }
         }

         break;
      }

   default:
      break;
   }

   self->m_GMP.sendto(ip, port, id, msg);

   //cout << "responded " << msg->getType() << " " << msg->m_iDataLength << endl;

   delete msg;
   delete (Param1*)p;

   return NULL;
}

void* CStore::remote(void* p)
{
   CStore* self = ((Param2*)p)->s;
   string filename = ((Param2*)p)->fn;
   UDTSOCKET u = ((Param2*)p)->u;
   int t = ((Param2*)p)->t;
   int conn = ((Param2*)p)->c;
   int mode = ((Param2*)p)->m;
   delete (Param2*)p;

   int32_t cmd;
   bool run = true;

/*
   // timed wait on accept!
   if (1 == conn)
   {
      timeval tv;
      UDT::UDSET readfds;

      tv.tv_sec = 60;
      tv.tv_usec = 0;

      UD_ZERO(&readfds);
      UD_SET(u, &readfds);

      int res = UDT::select(0, &readfds, NULL, NULL, &tv);

      if (UDT::ERROR == res)
         return NULL;
   }
   else
   {
      timeval tv;
      fd_set readfds;

      tv.tv_sec = 60;
      tv.tv_usec = 0;

      FD_ZERO(&readfds);
      FD_SET(t, &readfds);

      select(t+1, &readfds, NULL, NULL, &tv);

      if (!FD_ISSET(t, &readfds))
         return NULL;
   }
*/

   UDTSOCKET lu = u;
   int lt = t;

   if (1 == conn)
   {
      u = UDT::accept(u, NULL, NULL);
      UDT::close(lu);
   }
   else
   {
      t = accept(t, NULL, NULL);
      ::close(lt);
   }


   self->m_KBase.m_iNumConn ++;


   filename = self->m_strHomeDir + filename;

   timeval t1, t2;
   gettimeofday(&t1, 0);

   int64_t rb = 0;
   int64_t wb = 0;

   int32_t response = 0;

   while (run)
   {
      if (1 == conn)
      {
         if (UDT::recv(u, (char*)&cmd, 4, 0) < 0)
            continue;
      }
      else
      {
         if (::recv(t, (char*)&cmd, 4, 0) <= 0)
            continue;
      }

      if (4 != cmd)
      {
         if ((2 == cmd) || (5 == cmd))
         {
            if (0 == mode)
               response = -1;
         }
         else
            response = 0;

         if (1 == conn)
         {
            if (UDT::send(u, (char*)&response, 4, 0) < 0)
               continue;
         }
         else
         {
            if (::send(t, (char*)&response, 4, 0) < 0)
               continue;
         }

         if (-1 == response)
            continue;
      }

      switch (cmd)
      {
      case 1:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // READ LOCK

            int64_t param[2];

            ifstream ifs(filename.c_str(), ios::in | ios::binary);

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               if (UDT::sendfile(u, ifs, param[0], param[1]) < 0)
                  run = false;
               else
                  rb += param[1];
            }
            else
            {
               if (::recv(t, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               ifs.seekg(param[0]);

               int unit = 10240000;
               char* data = new char[unit];
               int ssize = 0;

               while (run && (ssize + unit <= param[1]))
               {
                  ifs.read(data, unit);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ts, unit - ts, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

                  ssize += unit;
               }

               if (ssize < param[1])
               {
                  ifs.read(data, param[1] - ssize);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ssize, param[1] - ssize, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

               }

               if (run)
                  rb += param[1];

               delete [] data;
            }

            ifs.close();

            // UNLOCK

            break;
         }

      case 2:
         {
            if (0 < (mode & 2))
               response = 0;
            else
               response = -1;

            // WRITE LOCK

            int64_t param[2];

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               ofstream ofs;
               ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);

               if (UDT::recvfile(u, ofs, param[0], param[1]) < 0)
                  run = false;
               else
                  wb += param[1];

               ofs.close();
            }
            else
            {
               if (::recv(t, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               char* temp = new char[param[1]];
               int rs = 0;
               while (rs < param[1])
               {
                  int r = ::recv(t, temp + rs, param[1] - rs, 0);
                  if (r < 0)
                  {
                     run = false;
                     break;
                  }

                  rs += r;
               }

               ofstream ofs;
               ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);
               ofs.seekp(param[0], ios::beg);
               ofs.write(temp, param[1]);
               ofs.close();

               delete [] temp;
               wb += param[1];
            }

            // UNLOCK

            break;
         }

      case 3:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // READ LOCK

            int64_t offset = 0;
            int64_t size = 0;

            ifstream ifs(filename.c_str(), ios::in | ios::binary);
            ifs.seekg(0, ios::end);
            size = (int64_t)(ifs.tellg());
            ifs.seekg(0, ios::beg);

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)&offset, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               size -= offset;

               if (UDT::send(u, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  ifs.close();
                  break;
               }

               if (UDT::sendfile(u, ifs, offset, size) < 0)
                  run = false;
               else
                  rb += size;
            }
            else
            {
               if (::recv(t, (char*)&offset, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               size -= offset;

               if (::send(t, (char*)&size, 8, 0) < 0)
                  run = false;

               int unit = 10240000;
               char* data = new char[unit];
               int ssize = 0;

               while (run && (ssize + unit <= size))
               {
                  ifs.read(data, unit);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ts, unit - ts, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

                  ssize += unit;
               }

               if (ssize < size)
               {
                  ifs.read(data, size - ssize);

                  int ts = 0;
                  while (ts < size - ssize)
                  {
                     int ss = ::send(t, data + ssize, size - ssize, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

               }

               delete [] data;

               if (run)
                  rb += size;
            }

            ifs.close();

            // UNLOCK

            break;
         }

      case 5:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // WRITE LOCK

            int64_t offset = 0;
            int64_t size = 0;

            ofstream ofs(filename.c_str(), ios::out | ios::binary | ios::trunc);

            if (1 == conn)
            {
               //if (UDT::recv(u, (char*)&offset, 8, 0) < 0)
               //{
               //   run = false;
               //   break;
               //}
               //offset = 0;

               if (UDT::recv(u, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               if (UDT::recvfile(u, ofs, offset, size) < 0)
                  run = false;
               else
                  wb += size;
            }
            else
            {
               if (::recv(t, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               const int unit = 1024000;
               char* data = new char [unit];
               int64_t rsize = 0;

               while (rsize < size)
               {
                  int rs = ::recv(t, data, (unit < size - rsize) ? unit : size - rsize, 0);

                  if (rs < 0)
                  {
                     run = false;
                     break;
                  }

                  ofs.write(data, rs);

                  rsize += rs;
               }

               delete [] data;

               if (run)
                  rb += size;
            }

            ofs.close();

            // UNLOCK
            break;
         }

      case 4:
         run = false;
         break;

      default:
         break;
      }
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;
   double avgRS = 0;
   double avgWS = 0;
   if (duration > 0)
   {
      avgRS = rb / duration * 8.0 / 1000000.0;
      avgWS = wb / duration * 8.0 / 1000000.0;
   }

   sockaddr_in addr;
   int addrlen = sizeof(addr);
   if (1 == conn)
      UDT::getpeername(u, (sockaddr*)&addr, &addrlen);
   else
      getpeername(t, (sockaddr*)&addr, (socklen_t*)&addrlen);
   char ip[64];
   inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
   int port = ntohs(addr.sin_port);
   
   self->m_PerfLog.insert(ip, port, filename.c_str(), duration, avgRS, avgWS);

   if (1 == conn)
      UDT::close(u);
   else
      close(t);

   self->m_KBase.m_iNumConn --;

   cout << "file server closed " << ip << " " << port << " " << avgRS << endl;

   return NULL;
}

void CStore::updateOutLink()
{
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_LocalFile.getFileList(filelist);

   CCBMsg msg;
   //msg.resize(65536);

   Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(500);

      // TO DO
      // check disk file for size update

      int fid = CDHash::hash(i->first.c_str(), m_iKeySpace);

      if (-1 == m_Router.lookup(fid, &loc))
         continue;

      set<CFileAttr, CAttrComp>::iterator attr = i->second.begin();

      // if the "loc" already have the file information, no need to update
      if (0 == strcmp(loc.m_pcIP, attr->m_pcNameHost))
         continue;

      // notify the current name holder to remove this file from its index
      if (strlen(attr->m_pcNameHost) > 0)
      {
         msg.setType(10);
         strcpy(msg.getData(), i->first.c_str());
         msg.m_iDataLength = 4 + strlen(i->first.c_str()) + 1;
         m_GMP.rpc(attr->m_pcNameHost, m_iCBFSPort, &msg, &msg);
      }

      // Dangerous const cast!!!
      strcpy((char*)attr->m_pcNameHost, loc.m_pcIP);
      const_cast<int&>(attr->m_iNamePort) = m_iCBFSPort;

      msg.setType(3);
      attr->synchronize(msg.getData(), msg.m_iDataLength);
      msg.m_iDataLength += 4;
      assert(msg.m_iDataLength < 1024);
      m_GMP.rpc(loc.m_pcIP, m_iCBFSPort, &msg, &msg);
   }
}

void CStore::updateInLink()
{
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_RemoteFile.getFileList(filelist);

   CCBMsg msg;
   msg.resize(65536);

   //Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(500);

      // check if the original file still exists
      int c = 0;
      for (set<CFileAttr, CAttrComp>::iterator j = i->second.begin(); j != i->second.end();)
      {
         msg.setType(6);
         msg.setData(0, j->m_pcName, strlen(j->m_pcName) + 1);
         msg.m_iDataLength = 4 + strlen(j->m_pcName) + 1;

         int r = m_GMP.rpc(j->m_pcHost, m_iCBFSPort, &msg, &msg);

         if ((r <= 0) || (msg.getType() < 0))
         {
            i->second.erase(j);
            j = i->second.begin();
            for (int k = 0; k < c; ++ k)
               ++ j;
         }
         else
         {
            ++ j;
            ++ c;
         }
      }

      if (i->second.size() == 0)
         m_RemoteFile.remove(i->first);
   }
}

int CStore::initLocalFile()
{
   ifstream ft("../conf/file.conf");

   if (ft.bad())
   {
      cout << "cannot locate configuration file. Please check ../conf/file.conf." << endl;
      return -1;
   }

   char buf[1024];

   // home directory information
   while (!ft.eof())
   {
      sleep(1);

      ft.getline(buf, 1024);

      if ((strlen(buf) > 0) && (buf[0] != '#'))
      {
         m_strHomeDir = buf;
         break;
      }
   }

   cout << "Home Dir " << m_strHomeDir << endl;

   CFileAttr attr;

   // initialize all files in the home directory, excluding "." and ".."

   dirent **namelist;
   int n = scandir(m_strHomeDir.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      perror("scandir");
   else 
   {
      for (int i = 0; i < n; ++ i) 
      {
         // skip ".", "..", and other reserved directory starting by '.'
         // skip directory

         struct stat s;
         stat((m_strHomeDir + namelist[i]->d_name).c_str(), &s);

         if ((namelist[i]->d_name[0] != '.') && (!S_ISDIR(s.st_mode)))
         {
            ifstream ifs((m_strHomeDir + namelist[i]->d_name).c_str());
            ifs.seekg(0, ios::end);
            int64_t size = ifs.tellg();
            ifs.close();

            strcpy(attr.m_pcName, namelist[i]->d_name);
            strcpy(attr.m_pcHost, m_strLocalHost.c_str());
            attr.m_iPort = m_iLocalPort;
            // original file is read only
            attr.m_iAttr = 1;
            attr.m_llSize = size;

            m_LocalFile.insert(attr);

            cout << "init local file... " << namelist[i]->d_name << " " << size << endl;
         }

         free(namelist[n]);
      }
      free(namelist);
   }

   return 1;
}

void CStore::updateNameIndex(int& next)
{
   // TODO
   // only updates changes after last update

   Node p;

   if (m_Router.lookup((unsigned int)(pow(double(2), next)), &p) < 0)
      return;

   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_LocalFile.getFileList(filelist);

   vector<string> files;
   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      files.insert(files.end(), i->first);

   CCBMsg msg;
   if (files.size() * 64 + 4 > msg.m_iBufLength)
      msg.resize(files.size() * 64 + 4);

   msg.setType(7);
   CNameIndex::synchronize(files, msg.getData(), msg.m_iDataLength);
   msg.m_iDataLength += 4;

   m_GMP.rpc(p.m_pcIP, m_iCBFSPort, &msg, &msg);

   ++ next;
   if (next == m_iKeySpace)
      next = 0;
}

int CStore::checkIndexLoc(const unsigned int& id)
{
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      if (0 == !((unsigned int)(pow(double(2), i))) & id)
         return i;
   }

   return -1;
}
