/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/25/2007
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
      *i = new char[size / m_iBucketNum];
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

   // processing...
   while (true)
   {
      int32_t dssize;
      if (datachn->recv((char*)&dssize, 4) < 0)
         break;
      char* dataseg = new char[dssize];
      if (datachn->recv(dataseg, dssize) < 0)
         break;

      // read data segment parameters
      string datafile = dataseg;
      int64_t offset = *(int64_t*)(dataseg + 64);
      int64_t totalrows = *(int64_t*)(dataseg + 72);
      int64_t* index = NULL;
      if (totalrows > 0)
         new int64_t[totalrows + 1];

      // read outupt parameters
      int buckets = *(int32_t*)(dataseg + 80);
      bool perm = 0;
      char* locations = NULL;
      if (buckets != 0)
      {
         perm = *(int32_t*)(dataseg + 84);

         if (buckets > 0)
         {
            locations = new char[dssize - 88];
            memcpy(locations, dataseg + 88, dssize - 88);
         }
      }
      string localfile = (buckets < 0) ? dataseg + 88 : "";
      delete [] dataseg;


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
      char* rdata;
      if (size < 1000000) 
         rdata = new char[1000000];
      else
         rdata = new char[size];

      int dlen = 0;
      int64_t* rindex = new int64_t[totalrows + 2];
      int ilen = 0;
      int bid;
      int progress = 0;

      // rdata initially contains home data directory
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

      cout << "RESULT " << dlen << " " << *(double*)(rdata + 340) << endl;


      cout << "completed 100 " << ip << " " << ctrlport << endl;
      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = 4 + 8;
      if (self->m_GMP.rpc(ip.c_str(), ctrlport, &msg, &msg) < 0)
         return NULL;

      cout << "sending data back... " << buckets << endl;
      self->SPESendResult(buckets, result, localfile, perm, datachn, locations);

      result.clear();
      delete [] locations;
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

   ofstream datafile((self->m_strHomeDir + localfile).c_str());
   ofstream indexfile((self->m_strHomeDir + localfile + ".idx").c_str());

   int64_t start = 0;
   indexfile.write((char*)&start, 8);

   for (int i = 0; i < dsnum; ++ i)
   {
      char speip[64];
      int speport;
      CCBMsg msg;
      int msgid;
      gmp->recvfrom(speip, speport, msgid, &msg);

      if (*(int32_t*)msg.getData() == 0)
      {
         gmp->sendto(speip, speport, msgid, &msg);
      }
      else if (*(int32_t*)msg.getData() == 1)
      {
         datafile.write((char*)*(int32_t*)(msg.getData() + 8), *(int32_t*)(msg.getData() + 4));

         int64_t* p = (int64_t*)*(int32_t*)(msg.getData() + 16);
         int len = *(int32_t*)(msg.getData() + 12) - 1;
         for (int i = 0; i < len; ++ i)
            *(++ p) += start;
         start = *p;
         indexfile.write((char*)(p - len + 1), len * 8);

         msg.m_iDataLength = 4;
         gmp->sendto(speip, speport, msgid, &msg);
      }
      else
      {
         Transport t;
         int dataport = 0;
         int remoteport = *(int32_t*)(msg.getData() + 4);
         t.open(dataport);

         *(int32_t*)msg.getData() = dataport;
         msg.m_iDataLength = 4 + 4;
         gmp->sendto(speip, speport, msgid, &msg);

         t.connect(speip, remoteport);

         int32_t len;
         t.recv((char*)&len, 4);
         char* data = new char[len];
         t.recv(data, len);
         datafile.write(data, len);
         t.recv((char*)&len, 4);
         int64_t* index = new int64_t[len];
         t.recv((char*)index, len * 8);
         for (int i = 1; i <= len; ++ i)
            index[i] += start;
         start = index[len];
         indexfile.write((char*)(index + 1), (len - 1) * 8);
         t.close();
      }
   }

   datafile.close();
   indexfile.close();

   gmp->close();
   delete gmp;

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

int Server::SPESendResult(const int& buckets, const SPEResult& result, const string& localfile, const bool& perm, Transport* datachn, char* locations)
{
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
         char* dstip = locations + i * 72;
         int dstport = *(int32_t*)(locations + i * 72 + 64);
         int32_t pass;
         CCBMsg msg;

         if (result.m_vDataLen[i] == 0)
         {
            pass = 0;
            msg.setData(0, (char*)&pass, 4);
            msg.m_iDataLength = 4 + 4;
            m_GMP.rpc(dstip, *(int32_t*)(locations + i * 72 + 68), &msg, &msg);
         }
         else if ((m_strLocalHost == dstip) && (m_iLocalPort == dstport))
         {
            pass = 1;
            msg.setData(0, (char*)&pass, 4);
            int size = result.m_vDataLen[i];
            msg.setData(4, (char*)&size, 4);
            int pos = (long)result.m_vData[i];
            msg.setData(8, (char*)&pos, 4);
            size = result.m_vIndexLen[i];
            msg.setData(12, (char*)&size, 4);
            pos = (long)result.m_vIndex[i];
            msg.setData(16, (char*)&pos, 4);

            msg.m_iDataLength = 4 + 20;

            m_GMP.rpc(dstip, *(int32_t*)(locations + i * 72 + 68), &msg, &msg);
         }
         else
         {
            Transport t;
            int dataport = 0;
            t.open(dataport);

            pass = 2;
            msg.setData(0, (char*)&pass, 4);
            msg.setData(4, (char*)&dataport, 4);
            msg.m_iDataLength = 4 + 8;

            m_GMP.rpc(locations + i * 72, *(int32_t*)(locations + i * 72 + 68), &msg, &msg);

            t.connect(locations + i * 72, *(int32_t*)msg.getData());

            int32_t size = result.m_vDataLen[i];
            t.send((char*)&size, 4);
            t.send(result.m_vData[i], size);
            size = result.m_vIndexLen[i];
            t.send((char*)&size, 4);
            t.send((char*)result.m_vIndex[i], result.m_vIndexLen[i] * 8);
            t.close();
         }

         sarray[i] = result.m_vDataLen[i];
         rarray[i] = result.m_vIndexLen[i] - 1;
      }

      // send back size and recnum information
      datachn->send((char*)sarray, buckets * 4);
      datachn->send((char*)rarray, buckets * 4);
      delete [] sarray;
      delete [] rarray;
   }

   return 1;
}
