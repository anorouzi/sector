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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/13/2008
*****************************************************************************/

#include <server.h>
#include <dlfcn.h>
#include <fsclient.h>
#include <dhash.h>

using namespace cb;

SPEResult::~SPEResult()
{
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
      delete [] *i;
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      delete [] *i;
}

void SPEResult::init(const int& n, const int& rows, const int& size)
{
   if (n < 1)
     m_iBucketNum = 1;
   else
     m_iBucketNum = n;

   m_iSize = size;

   m_vIndex.resize(m_iBucketNum);
   m_vIndexLen.resize(m_iBucketNum);
   m_vData.resize(m_iBucketNum);
   m_vDataLen.resize(m_iBucketNum);

   for (vector<int32_t>::iterator i = m_vIndexLen.begin(); i != m_vIndexLen.end(); ++ i)
      *i = 1;
   for (vector<int32_t>::iterator i = m_vDataLen.begin(); i != m_vDataLen.end(); ++ i)
      *i = 0;
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
   {
      *i = new int64_t[rows + 2];
      (*i)[0] = 0;
   }
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      *i = new char[size];
}

void SPEResult::addData(const int& bucketid, const int64_t* index, const int64_t& ilen, const char* data, const int64_t& dlen)
{
   if ((bucketid >= m_iBucketNum) || (bucketid < 0))
      return;

   int64_t* p = m_vIndex[bucketid] + m_vIndexLen[bucketid] - 1;
   int64_t start = *p;
   for (int i = 1; i <= ilen; ++ i)
      *(++ p) = index[i] + start;

   m_vIndexLen[bucketid] += ilen;

   memcpy(m_vData[bucketid] + m_vDataLen[bucketid], data, dlen);
   m_vDataLen[bucketid] += dlen;
}

void SPEResult::clear()
{
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
   {
      delete [] *i;
      *i = NULL;
   }
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
   {
      delete [] *i;
      *i = NULL;
   }
}

void* Server::SPEHandler(void* p)
{
   Server* self = ((Param4*)p)->serv_instance;
   Transport* datachn = ((Param4*)p)->datachn;
   const string ip = ((Param4*)p)->client_ip;
   const int ctrlport = ((Param4*)p)->client_ctrl_port;
   const int dataport = ((Param4*)p)->client_data_port;
   const int speid = ((Param4*)p)->speid;
   const string function = ((Param4*)p)->function;
   const int rows = ((Param4*)p)->rows;
   const char* param = ((Param4*)p)->param;
   const int psize = ((Param4*)p)->psize;
   delete (Param4*)p;

   CCBMsg msg;


   cout << "rendezvous connect " << ip << " " << dataport << endl;
   if (datachn->connect(ip.c_str(), dataport) < 0)
      return NULL;

   // read outupt parameters
   int buckets;
   if (datachn->recv((char*)&buckets, 4) < 0)
      return NULL;

   char* outputloc = NULL;
   string localfile = "";
   if (buckets > 0)
   {
      outputloc = new char[buckets * 72];
      if (datachn->recv(outputloc, buckets * 72) < 0)
         return NULL;
   }
   else if (buckets < 0)
   {
      outputloc = new char[64];
      if (datachn->recv(outputloc, 64) < 0)
         return NULL;
      localfile = outputloc;
   }

   map<Node, Transport*, NodeComp> OutputChn;


   // initialize processing function
   string dir;
   self->m_LocalFile.lookup(function + ".so", dir);
   void* handle = dlopen((self->m_strHomeDir + dir + function + ".so").c_str(), RTLD_LAZY);
   if (NULL == handle)
      return NULL;
   int (*process)(const char*, const int&, const int64_t*, char*, int&, int&, int64_t*, int&, const char*, const int&);
   process = (int (*) (const char*, const int&, const int64_t*, char*, int&, int&, int64_t*, int&, const char*, const int&) )dlsym(handle, function.c_str());
   if (NULL == process)
   {
      cerr << dlerror() <<  endl;
      return NULL;
   }


   timeval t1, t2, t3, t4;
   gettimeofday(&t1, 0);

   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(speid), 4);

   char* dataseg = new char[80];

   // processing...
   while (true)
   {
      if (datachn->recv(dataseg, 80) < 0)
         break;

      // read data segment parameters
      string datafile = dataseg;
      int64_t offset = *(int64_t*)(dataseg + 64);
      int64_t totalrows = *(int64_t*)(dataseg + 72);
      int64_t* index = NULL;
      if (totalrows > 0)
         index = new int64_t[totalrows + 1];

      int size = 0;
      char* block = NULL;
      int unitrows = (rows != -1) ? rows : totalrows;

      cout << "new job " << datafile << " " << offset << " " << totalrows << endl;

      // read data
      if (0 != rows)
      {
         if (self->SPEReadData(datafile, offset, size, index, totalrows, block) <= 0)
         {
            delete [] index;
            delete [] block;
            // acknowlege error here...
            continue;
         }
      }
      else
      {
         // store file name in "process" parameter
         block = new char[64];
         strcpy(block, datafile.c_str());
         size = datafile.length() + 1;
         totalrows = 0;
      }

      // TODO: use dynamic size at run time!
      char* rdata = NULL;
      if (size < 1000000) 
         rdata = new char[1000000];
      else
         rdata = new char[size];

      int dlen = 0;
      int64_t* rindex = NULL;
      rindex = new int64_t[totalrows + 2];
      int ilen = 0;
      int bid;
      int progress = 0;

      // rdata initially contains home data directory
      self->m_LocalFile.lookup(datafile, dir);
      strcpy(rdata, (self->m_strHomeDir + dir).c_str());

      SPEResult result;
      result.init(buckets, totalrows, (size > 1000000) ? size : 1000000);

      gettimeofday(&t3, 0);
      for (int i = 0; i < totalrows; i += unitrows)
      {
         if (unitrows > totalrows - i)
            unitrows = totalrows - i;

         process(block + index[i] - index[0], unitrows, index + i, rdata, dlen, ilen, rindex, bid, param, psize);
         if (buckets <= 0)
            bid = 0;

         result.addData(bid, rindex, ilen, rdata, dlen);

         gettimeofday(&t4, 0);
         if (t4.tv_sec - t3.tv_sec > 1)
         {
            progress = i * 100 / totalrows;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = 4 + 8;
            if (self->m_GMP.rpc(ip.c_str(), ctrlport, &msg, &msg) < 0)
               return NULL;
            t3 = t4;
         }
      }

      if (0 == unitrows)
      {
         process(block, 0, NULL, rdata, dlen, ilen, rindex, bid, param, psize);
         result.addData(bid, rindex, ilen, rdata, dlen);
      }

      cout << "completed 100 " << ip << " " << ctrlport << endl;
      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = 4 + 8;
      if (self->m_GMP.rpc(ip.c_str(), ctrlport, &msg, &msg) < 0)
         break;

      //cout << "sending data back... " << buckets << endl;
      self->SPESendResult(speid, buckets, result, localfile, datachn, outputloc, &OutputChn);

      result.clear();
      delete [] index;
      delete [] block;
      delete [] rdata;
      delete [] rindex;

      index = NULL;
      block = NULL;
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;

   dlclose(handle);
   datachn->close();
   delete datachn;

   cout << "comp server closed " << ip << " " << ctrlport << " " << duration << endl;

   delete [] param;
   delete [] dataseg;

   for (map<Node, Transport*, NodeComp>::iterator i = OutputChn.begin(); i != OutputChn.end(); ++ i)
   {
      i->second->close();
      delete i->second;
   }

   return NULL;
}

void* Server::SPEShuffler(void* p)
{
   Server* self = ((Param5*)p)->serv_instance;
   string ip = ((Param5*)p)->client_ip;
   //int port = ((Param5*)p)->client_ctrl_port;
   string localfile = ((Param5*)p)->filename;
   int dsnum = ((Param5*)p)->dsnum;
   CGMP* gmp = ((Param5*)p)->gmp;
   delete (Param5*)p;

   // remove old result data files
   for (int i = 0; i < dsnum; ++ i)
   {
      char tmp[64];
      sprintf(tmp, "%s.%d", (self->m_strHomeDir + localfile).c_str(), i);
      unlink(tmp);
      sprintf(tmp, "%s.%d.idx", (self->m_strHomeDir + localfile).c_str(), i);
      unlink(tmp);
   }

   // index file initial offset
   vector<int64_t> offset;
   offset.resize(dsnum);
   for (vector<int64_t>::iterator i = offset.begin(); i != offset.end(); ++ i)
      *i = 0;

   // data channels
   map<Node, Transport*, NodeComp> DataChn;

   while (true)
   {
      char speip[64];
      int speport;
      CCBMsg msg;
      int msgid;
      gmp->recvfrom(speip, speport, msgid, &msg);

      int pass = *(int32_t*)msg.getData();

      if (0 == pass)
      {
         gmp->sendto(speip, speport, msgid, &msg);
         continue;
      }
      else if (pass < 0)
      {
         // client send a message to stop the shuffler
         gmp->sendto(speip, speport, msgid, &msg);
         break;
      }

      int bucket = *(int32_t*)(msg.getData() + 4);
      int speid = *(int32_t*)(msg.getData() + 8);

      char tmp[64];
      sprintf(tmp, "%s.%d", (self->m_strHomeDir + localfile).c_str(), bucket);
      ofstream datafile(tmp, ios::app);

      sprintf(tmp, "%s.%d.idx", (self->m_strHomeDir + localfile).c_str(), bucket);
      ofstream indexfile(tmp, ios::app);
      int64_t start = offset[bucket];
      if (0 == start)
         indexfile.write((char*)&start, 8);

      if (1 == pass)
      {
         datafile.write((char*)*(int64_t*)(msg.getData() + 12), *(int32_t*)(msg.getData() + 8));

         int64_t* p = (int64_t*)*(int64_t*)(msg.getData() + 24);
         int len = *(int32_t*)(msg.getData() + 20);

         for (int i = 0; i < len; ++ i)
            *(++ p) += start;
         offset[bucket] = *p;
         indexfile.write((char*)(p - len), len * 8);

         msg.m_iDataLength = 4;
         gmp->sendto(speip, speport, msgid, &msg);
      }
      else
      {
         Node n;
         strcpy(n.m_pcIP, speip);
         n.m_iAppPort = speid;

         Transport* chn = NULL;

         map<Node, Transport*, NodeComp>::iterator i = DataChn.find(n);
         if (i != DataChn.end())
         {
            chn = i->second;
            msg.m_iDataLength = 4;
            gmp->sendto(speip, speport, msgid, &msg);
         }
         else
         {
            Transport* t = new Transport;
            int dataport = 0;
            int remoteport = *(int32_t*)(msg.getData() + 12);
            t->open(dataport);

            *(int32_t*)msg.getData() = dataport;
            msg.m_iDataLength = 4 + 4;
            gmp->sendto(speip, speport, msgid, &msg);

            t->connect(speip, remoteport);

            DataChn[n] = t;
            chn = t;
         }

         int32_t len;
         chn->recv((char*)&len, 4);
         char* data = new char[len];
         chn->recv(data, len);
         datafile.write(data, len);
         delete [] data;
         chn->recv((char*)&len, 4);
         int64_t* index = new int64_t[len];
         chn->recv((char*)index, len * 8);
         for (int i = 0; i < len; ++ i)
            index[i] += start;
         offset[bucket] = index[len - 1];
         indexfile.write((char*)index, len * 8);
         delete [] index;
      }

      datafile.close();
      indexfile.close();
   }

   gmp->close();
   delete gmp;

   // release data channels
   for (map<Node, Transport*, NodeComp>::iterator i = DataChn.begin(); i != DataChn.end(); ++ i)
   {
      i->second->close();
      delete i->second;
   }

   self->scanLocalFile();

   return NULL;
}

int Server::SPEReadData(const string& datafile, const int64_t& offset, int& size, int64_t* index, const int64_t& totalrows, char*& block)
{
   string dir;

   if (m_LocalFile.lookup(datafile.c_str(), dir) > 0)
   {
      ifstream idx;
      idx.open((m_strHomeDir + dir + datafile + ".idx").c_str());
      idx.seekg(offset * 8);
      idx.read((char*)index, (totalrows + 1) * 8);
      idx.close();

      size = index[totalrows] - index[0];
      block = new char[size];

      ifstream ifs;
      ifs.open((m_strHomeDir + dir + datafile).c_str());
      ifs.seekg(index[0]);
      ifs.read(block, size);
      ifs.close();

      return totalrows;
   }
   else
   {
      File* f = Client::createFileHandle();
      if (f->open(datafile.c_str()) < 0)
         return -1;

      if (f->readridx((char*)index, offset, totalrows) < 0)
         return -1;

      size = index[totalrows] - index[0];
      block = new char[size];

      if (f->read(block, index[0], size) < 0)
         return -1;

      f->close();
      Client::releaseFileHandle(f);

      return totalrows;
   }

   return 0;
}

int Server::SPESendResult(const int& speid, const int& buckets, const SPEResult& result, const string& localfile, Transport* datachn, char* locations, map<Node, Transport*, NodeComp>* outputchn)
{
   bool perm = false;

   if (buckets == -1)
   {
      string dir = (perm) ? ".sector-fs/" : ".sphere/";
      m_SectorFS.create(localfile, DHash::hash(localfile.c_str(), m_iKeySpace), dir);

      ofstream ofs;
      ofs.open((m_strHomeDir + dir + localfile).c_str());
      ofs.write(result.m_vData[0], result.m_vDataLen[0]);
      ofs.close();
      ofs.open((m_strHomeDir + dir + localfile + ".idx").c_str());
      ofs.write((char*)result.m_vIndex[0], result.m_vIndexLen[0] * 8);
      ofs.close();

      scanLocalFile();

      // send back result file/record size
      int32_t size = result.m_vDataLen[0];
      datachn->send((char*)&size, 4);
      size = result.m_vIndexLen[0];
      datachn->send((char*)&size, 4);
   }
   else if (buckets == 0)
   {
      int32_t size = result.m_vDataLen[0];
      datachn->send((char*)&size, 4);
      datachn->send(result.m_vData[0], result.m_vDataLen[0]);
      size = result.m_vIndexLen[0];
      datachn->send((char*)&size, 4);
      datachn->send((char*)result.m_vIndex[0], result.m_vIndexLen[0] * 8);
   }
   else
   {
      int* sarray = new int[buckets];
      int* rarray = new int[buckets];
      for (int i = 0; i < buckets; ++ i)
      {
         sarray[i] = result.m_vDataLen[i];
         rarray[i] = result.m_vIndexLen[i] - 1;
      }
      // send back size and recnum information
      datachn->send((char*)sarray, buckets * 4);
      datachn->send((char*)rarray, buckets * 4);
      delete [] sarray;
      delete [] rarray;

      for (int i = 0; i < buckets; ++ i)
      {
         if (0 == result.m_vDataLen[i])
            continue;

         char* dstip = locations + i * 72;
         int32_t dstport = *(int32_t*)(locations + i * 72 + 64);
         int32_t shufflerport = *(int32_t*)(locations + i * 72 + 68);
         int32_t pass;
         CCBMsg msg;

         if ((m_strLocalHost == dstip) && (m_iLocalPort == dstport))
         {
            pass = 1;
            msg.setData(0, (char*)&pass, 4);
            msg.setData(4, (char*)&i, 4);
            int32_t size = result.m_vDataLen[i];
            msg.setData(8, (char*)&size, 4);
            uint64_t pos = (unsigned long)result.m_vData[i];
            msg.setData(12, (char*)&pos, 8);
            size = result.m_vIndexLen[i] - 1;
            msg.setData(20, (char*)&size, 4);
            pos = (unsigned long)(result.m_vIndex[i] + 1);
            msg.setData(24, (char*)&pos, 8);

            msg.m_iDataLength = 4 + 32;

            m_GMP.rpc(dstip, shufflerport, &msg, &msg);
         }
         else
         {
            pass = 2;
            msg.setData(0, (char*)&pass, 4);
            msg.setData(4, (char*)&i, 4);
            msg.setData(8, (char*)&speid, 4);

            Transport* chn;

            Node n;
            strcpy(n.m_pcIP, dstip);
            n.m_iAppPort = shufflerport;

            map<Node, Transport*, NodeComp>::iterator c = outputchn->find(n);
            if (c != outputchn->end())
            {
               msg.m_iDataLength = 4 + 12;
               m_GMP.rpc(dstip, shufflerport, &msg, &msg);

               chn = c->second;
            }
            else
            {
               Transport* t = new Transport;
               int dataport = 0;
               t->open(dataport);

               msg.setData(12, (char*)&dataport, 4);
               msg.m_iDataLength = 4 + 16;

               m_GMP.rpc(dstip, shufflerport, &msg, &msg);

               t->connect(dstip, *(int32_t*)msg.getData());

               (*outputchn)[n] = t;
               chn = t;
            }

            int32_t size = result.m_vDataLen[i];
            chn->send((char*)&size, 4);
            chn->send(result.m_vData[i], size);
            size = result.m_vIndexLen[i] - 1;
            chn->send((char*)&size, 4);
            chn->send((char*)(result.m_vIndex[i] + 1), size * 8);
         }
      }
   }

   return 1;
}
