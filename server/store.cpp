#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/sendfile.h>
#include <store.h>

using namespace std;


CStore::CStore(const string& ip)
{
   m_strLocalHost = ip;
   m_iLocalPort = 7000;

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
      res = m_Router.join(m_strLocalHost.c_str(), ip, 2257);
   }

   if (res < 0)
      return -1;

   if (initLocalFile() < 0)
      return -1;

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, run, this);
   pthread_detach(msgserver);

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
               int r = (int)(filelist.size() * rand() / double(RAND_MAX));
               set<CFileAttr, CAttrComp>::iterator i = filelist.begin();
               for (int j = 0; j < r; ++ j)
                  ++ i;

               msg->setData(0, i->m_pcHost, strlen(i->m_pcHost) + 1);
               msg->setData(64, (char*)(&i->m_iPort), 4);
               msg->m_iDataLength = 4 + 64 + 4;

               cout << "locate " << filename << " " << filelist.size() << " " << i->m_pcHost << " " << i->m_iPort << endl;
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 2: // open file
         {
            char* filename = msg->getData();

            int conn = *(int*)(msg->getData() + 64);

            int mode = *(int*)(msg->getData() + 68);
            // TO DO
            // check ownership and previlege;

            if (self->m_LocalFile.lookup(filename, NULL) < 0)
            {
               CFileAttr attr;
               memcpy(attr.m_pcName, filename, 64);
               attr.m_llSize = 0;
               memcpy(attr.m_pcHost, self->m_strLocalHost.c_str(), 64);
               attr.m_iPort = self->m_iLocalPort;
               self->m_LocalFile.insert(attr);
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

            msg->m_iDataLength = 4;

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
               self->m_NameIndex.insert(msg->getData() + i * 64, ip, 7000);
            }

            //cout << "global name index updated " << (msg->m_iDataLength - 4) / 64 << endl;

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
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

            if (self->m_GMP.rpc(n.m_pcIP, 7000, msg, msg) < 0)
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
               if (self->m_GMP.rpc(n.m_pcIP, 7000, msg, msg) < 0)
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

   filename = self->m_strHomeDir + filename;

   timeval t1, t2;
   gettimeofday(&t1, 0);

   int64_t rb = 0;
   int64_t wb = 0;

   while (run)
   {
      if (1 == conn)
         UDT::recv(u, (char*)&cmd, 4, 0);
      else
         recv(t, (char*)&cmd, 4, 0);

      switch (cmd)
      {
      case 1:
         {
            //TODO
            // check mode

            // READ LOCK

            int64_t param[2];

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               ifstream ifs(filename.c_str(), ios::in | ios::binary);

               if (UDT::sendfile(u, ifs, param[0], param[1]) < 0)
                  run = false;
               else
                  rb += param[1];

               ifs.close();
            }
            else
            {
               if (::recv(t, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               int fd = open(filename.c_str(), O_RDONLY);

               if (::sendfile(t, fd, (off_t*)param, param[1]) < 0)
                  run = false;
               else
                  rb += param[1];

               ::close(fd);
            }

            // UNLOCK

            break;
         }

      case 2:
         {
            //TODO
            // check mode

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
            //TODO
            // check mode

            // READ LOCK

            int64_t offset = 0;
            int64_t size = 0;

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)&offset, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               ifstream ifs(filename.c_str(), ios::in | ios::binary);
               ifs.seekg(0, ios::end);
               size = (int64_t)(ifs.tellg()) - offset;
               ifs.seekg(0, ios::beg);

               if (UDT::send(u, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  ifs.close();
                  break;
               }

               if (UDT::sendfile(u, (ifstream&)ifs, offset, size) < 0)
                  run = false;
               else
                  rb += size;

               ifs.close();
            }
            else
            {
               int fd = open(filename.c_str(), O_RDONLY);
               ///////////TODO find file length!

               if (::send(t, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  ::close(fd);
                  break;
               }

               if (::send(t, (char*)&size, 8, 0) < 0)
                  run = false;

               if (::sendfile(t, fd, (off_t*)&offset, size) < 0)
                  run = false;
               else
                  rb += size;
            }

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

   cout << "file server closed " << ip << " " << port << " " << avgRS << endl;

   return NULL;
}

void CStore::updateOutLink()
{
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_LocalFile.getFileList(filelist);

   CCBMsg msg;
   msg.resize(65536);

   Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(1000);

      // TO DO
      // check disk file for size update
      // update file.conf

      int fid = CDHash::hash(i->first.c_str(), m_iKeySpace);

      if (-1 == m_Router.lookup(fid, &loc))
         continue;

      msg.setType(3);
      i->second.begin()->synchronize(msg.getData(), msg.m_iDataLength);
      msg.m_iDataLength += 4;
      m_GMP.rpc(loc.m_pcIP, 7000, &msg, &msg);
   }
}

void CStore::updateInLink()
{
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_RemoteFile.getFileList(filelist);

   CCBMsg msg;
   msg.resize(65536);

   Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(1000);

      int fid = CDHash::hash(i->first.c_str(), m_iKeySpace);

      if (-1 == m_Router.lookup(fid, &loc))
         continue;

      if (strcmp(loc.m_pcIP, m_strLocalHost.c_str()) != 0)
      {
         m_RemoteFile.remove(i->first);
         continue;
      }

      int c = 0;
      for (set<CFileAttr, CAttrComp>::iterator j = i->second.begin(); j != i->second.end();)
      {
         msg.setType(6);
         msg.setData(0, j->m_pcName, strlen(j->m_pcName) + 1);
         msg.m_iDataLength = 4 + strlen(j->m_pcName) + 1;

         int r = m_GMP.rpc(j->m_pcHost, 7000, &msg, &msg);

         if ((r <=0) || (msg.getType() < 0))
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

   // file list
/*
   while (!ft.eof())
   {
      ft.getline(buf, 1024);

      if ((strlen(buf) > 0) && (buf[0] != '#'))
      {
         cout << "load file " <<  buf << endl;

         ifstream ifs((m_strHomeDir+buf).c_str());
         ifs.seekg(0, ios::end);
         int64_t size = ifs.tellg();
         ifs.close();

         strcpy(attr.m_pcName, buf);
         strcpy(attr.m_pcHost, m_strLocalHost.c_str());
         attr.m_iPort = m_iLocalPort;
         attr.m_llSize = size;

         m_LocalFile.insert(attr);
      }
   }
*/

   dirent **namelist;
   int n = scandir(m_strHomeDir.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      perror("scandir");
   else 
   {
      for (int i = 0; i < n; ++ i) 
      {
         if (namelist[i]->d_name[0] != '.')
         {
            ifstream ifs((m_strHomeDir + namelist[i]->d_name).c_str());
            ifs.seekg(0, ios::end);
            int64_t size = ifs.tellg();
            ifs.close();

            strcpy(attr.m_pcName, namelist[i]->d_name);
            strcpy(attr.m_pcHost, m_strLocalHost.c_str());
            attr.m_iPort = m_iLocalPort;
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
   Node p;

   if (m_Router.lookup((unsigned int)(pow(double(2), next)), &p) < 0)
      return;

   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_LocalFile.getFileList(filelist);

   vector<string> files;
   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      files.insert(files.end(), i->first.c_str());

   CCBMsg msg;
   msg.resize(65536);

   msg.setType(7);
   CNameIndex::synchronize(files, msg.getData(), msg.m_iDataLength);
   msg.m_iDataLength += 4;

   m_GMP.rpc(p.m_pcIP, 7000, &msg, &msg);

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
