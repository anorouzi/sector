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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/18/2008
*****************************************************************************/

#include <slave.h>
#include <sphere.h>
#include <dlfcn.h>
#include <iostream>

using namespace std;

SPEResult::~SPEResult()
{
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
      delete [] *i;
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      delete [] *i;
}

void SPEResult::init(const int& n)
{
   if (n < 1)
     m_iBucketNum = 1;
   else
     m_iBucketNum = n;

   m_vIndex.resize(m_iBucketNum);
   m_vIndexLen.resize(m_iBucketNum);
   m_vIndexPhyLen.resize(m_iBucketNum);
   m_vData.resize(m_iBucketNum);
   m_vDataLen.resize(m_iBucketNum);
   m_vDataPhyLen.resize(m_iBucketNum);

   for (vector<int32_t>::iterator i = m_vIndexLen.begin(); i != m_vIndexLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vDataLen.begin(); i != m_vDataLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vIndexPhyLen.begin(); i != m_vIndexPhyLen.end(); ++ i)
      *i = 0;
   for (vector<int32_t>::iterator i = m_vDataPhyLen.begin(); i != m_vDataPhyLen.end(); ++ i)
      *i = 0;
   for (vector<int64_t*>::iterator i = m_vIndex.begin(); i != m_vIndex.end(); ++ i)
      *i = NULL;
   for (vector<char*>::iterator i = m_vData.begin(); i != m_vData.end(); ++ i)
      *i = NULL;
}

void SPEResult::addData(const int& bucketid, const char* data, const int64_t& len)
{
   if ((bucketid >= m_iBucketNum) || (bucketid < 0) || (len <= 0))
      return;

   // dynamically increase index buffer size
   if (m_vIndexLen[bucketid] >= m_vIndexPhyLen[bucketid])
   {
      int64_t* tmp = new int64_t[m_vIndexPhyLen[bucketid] + 256];

      if (NULL != m_vIndex[bucketid])
      {
         memcpy((char*)tmp, (char*)m_vIndex[bucketid], m_vIndexLen[bucketid] * 8);
         delete [] m_vIndex[bucketid];
      }
      else
      {
         tmp[0] = 0;
         m_vIndexLen[bucketid] = 1;
      }
      m_vIndex[bucketid] = tmp;
      m_vIndexPhyLen[bucketid] += 256;
   }

   m_vIndex[bucketid][m_vIndexLen[bucketid]] = m_vIndex[bucketid][m_vIndexLen[bucketid] - 1] + len;
   m_vIndexLen[bucketid] ++;

   // dynamically increase index buffer size
   while (m_vDataLen[bucketid] + len > m_vDataPhyLen[bucketid])
   {
      char* tmp = new char[m_vDataPhyLen[bucketid] + 65536];

      if (NULL != m_vData[bucketid])
      {
         memcpy((char*)tmp, (char*)m_vData[bucketid], m_vDataLen[bucketid]);
         delete [] m_vData[bucketid];
      }
      m_vData[bucketid] = tmp;
      m_vDataPhyLen[bucketid] += 65536;
   }

   memcpy(m_vData[bucketid] + m_vDataLen[bucketid], data, len);
   m_vDataLen[bucketid] += len;
}

void SPEResult::clear()
{
   for (vector<int32_t>::iterator i = m_vIndexLen.begin(); i != m_vIndexLen.end(); ++ i)
   {
      if (*i > 0)
         *i = 1;
   }
   for (vector<int32_t>::iterator i = m_vDataLen.begin(); i != m_vDataLen.end(); ++ i)
      *i = 0;
}

void* Slave::SPEHandler(void* p)
{
   Slave* self = ((Param4*)p)->serv_instance;
   Transport* datachn = ((Param4*)p)->datachn;
   const int transid = ((Param4*)p)->transid;
   const string ip = ((Param4*)p)->client_ip;
   const int ctrlport = ((Param4*)p)->client_ctrl_port;
   const int dataport = ((Param4*)p)->client_data_port;
   const int speid = ((Param4*)p)->speid;
   const int key = ((Param4*)p)->key;
   const string function = ((Param4*)p)->function;
   const int rows = ((Param4*)p)->rows;
   const char* param = ((Param4*)p)->param;
   const int psize = ((Param4*)p)->psize;
   delete (Param4*)p;

   SectorMsg msg;

   cout << "rendezvous connect " << ip << " " << dataport << endl;
   if (datachn->connect(ip.c_str(), dataport) < 0)
      return NULL;
   cout << "connected\n";

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
      int32_t size = 0;
      if (datachn->recv((char*)&size, 4) < 0)
         return NULL;

      outputloc = new char[size];
      if (datachn->recv(outputloc, size) < 0)
         return NULL;
      localfile = outputloc;
   }

   map<Address, Transport*, AddrComp> OutputChn;


   // initialize processing function
   self->acceptLibrary(key, datachn);

   char path[64];
   sprintf(path, "%d", key);
   void* handle = dlopen((self->m_strHomeDir + ".sphere/" + path + "/" + function + ".so").c_str(), RTLD_LAZY);
   if (NULL == handle)
   {
      cerr << dlerror() << endl;
      return NULL;
   }
   int (*process)(const SInput*, SOutput*, SFile*);
   process = (int (*) (const SInput*, SOutput*, SFile*) )dlsym(handle, function.c_str());
   if (NULL == process)
   {
      cerr << dlerror() << endl;
      return NULL;
   }


   timeval t1, t2, t3, t4;
   gettimeofday(&t1, 0);

   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(speid), 4);

   SPEResult result;
   result.init(buckets);

   // processing...
   while (true)
   {
      int size = 0;
      if (datachn->recv((char*)&size, 4) < 0)
         break;
      char* dataseg = new char[size];
      if (datachn->recv(dataseg, size) < 0)
         break;

      // read data segment parameters
      int64_t offset = *(int64_t*)(dataseg);
      int64_t totalrows = *(int64_t*)(dataseg + 8);
      int32_t dsid = *(int32_t*)(dataseg + 16);
      string datafile = dataseg + 20;
      delete [] dataseg;
      cout << "new job " << datafile << " " << offset << " " << totalrows << endl;

      int64_t* index = NULL;
      if (totalrows > 0)
         index = new int64_t[totalrows + 1];
      char* block = NULL;
      int unitrows = (rows != -1) ? rows : totalrows;
      int progress = 0;

      // read data
      if (0 != rows)
      {
         size = 0;
         if (self->SPEReadData(datafile, offset, size, index, totalrows, block) <= 0)
         {
            delete [] index;
            delete [] block;

            progress = -1;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
            int id = 0;
            self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

            continue;
         }
      }
      else
      {
         // store file name in "process" parameter
         block = new char[datafile.length() + 1];
         strcpy(block, datafile.c_str());
         size = datafile.length() + 1;
         totalrows = 0;
      }

      // TODO: use dynamic size at run time!
      char* rdata = NULL;
      if (size < 1000000)
         size = 1000000;
      rdata = new char[size];

      int64_t* rindex = NULL;
      int* rbucket = NULL;
      if (totalrows < 65536)
      {
         rindex = new int64_t[65536];
         rbucket = new int[65536];
      }
      else
      {
         rindex = new int64_t[totalrows + 2];
         rbucket = new int[totalrows + 2];
      }

      SInput input;
      input.m_pcUnit = NULL;
      input.m_pcParam = (char*)param;
      input.m_iPSize = psize;
      SOutput output;
      output.m_pcResult = rdata;
      output.m_iBufSize = size;
      output.m_pllIndex = rindex;
      output.m_iIndSize = (totalrows < 65536) ? 65536 : totalrows + 2;
      output.m_piBucketID = rbucket;
      SFile file;
      file.m_strHomeDir = self->m_strHomeDir;
      file.m_strLibDir = self->m_strHomeDir + ".sphere/" + path + "/";
      file.m_strTempDir = self->m_strHomeDir + ".tmp/";

      result.clear();
      gettimeofday(&t3, 0);

      for (int i = 0; i < totalrows; i += unitrows)
      {
         if (unitrows > totalrows - i)
            unitrows = totalrows - i;

         input.m_pcUnit = block + index[i] - index[0];
         input.m_iRows = unitrows;
         input.m_pllIndex = index + i;

         process(&input, &output, &file);

         for (int r = 0; r < output.m_iRows; ++ r)
            result.addData(output.m_piBucketID[r], output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);

         gettimeofday(&t4, 0);
         if (t4.tv_sec - t3.tv_sec > 1)
         {
            progress = i * 100 / totalrows;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
            int id = 0;
            self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

            t3 = t4;
         }
      }

      if (0 == unitrows)
      {
         input.m_pcUnit = block;
         process(&input, &output, &file);
         for (int r = 0; r < output.m_iRows; ++ r)
            result.addData(output.m_piBucketID[r], output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
      }

      cout << "completed 100 " << ip << " " << ctrlport << endl;
      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
      int id = 0;
      self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

      cout << "sending data back... " << buckets << endl;
      char localfileid[64];
      localfileid[0] = '\0';
      if (buckets < 0)
         sprintf(localfileid, ".%d", dsid);
      self->SPESendResult(speid, buckets, result, localfile + localfileid, datachn, outputloc, &OutputChn);

      // report new files
      for (set<string>::iterator i = file.m_sstrFiles.begin(); i != file.m_sstrFiles.end(); ++ i)
         self->report(0, *i, true);

      delete [] index;
      delete [] block;
      delete [] rdata;
      delete [] rindex;
      delete [] rbucket;

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
   delete [] outputloc;

   for (map<Address, Transport*, AddrComp>::iterator i = OutputChn.begin(); i != OutputChn.end(); ++ i)
   {
      //i->second->close();
      delete i->second;
   }

   self->reportSphere(transid);

   return NULL;
}

void* Slave::SPEShuffler(void* p)
{
   Slave* self = ((Param5*)p)->serv_instance;
   int transid = ((Param5*)p)->transid;
   string client_ip = ((Param5*)p)->client_ip;
   int client_port = ((Param5*)p)->client_ctrl_port;
   string path = ((Param5*)p)->path;
   string localfile = ((Param5*)p)->filename;
   int bucketnum = ((Param5*)p)->bucket;
   CGMP* gmp = ((Param5*)p)->gmp;
   delete (Param5*)p;

   cout << "SPE Shuffler " << path << " " << localfile << " " << bucketnum << endl;

   ::mkdir((self->m_strHomeDir + path).c_str(), S_IRWXU);

   // remove old result data files
   for (int i = 0; i < bucketnum; ++ i)
   {
      char* tmp = new char[self->m_strHomeDir.length() + path.length() + localfile.length() + 64];
      sprintf(tmp, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), i);
      unlink(tmp);
      sprintf(tmp, "%s.%d.idx", (self->m_strHomeDir + path + "/" + localfile).c_str(), i);
      unlink(tmp);
      delete [] tmp;
   }

   // index file initial offset
   vector<int64_t> offset;
   offset.resize(bucketnum);
   for (vector<int64_t>::iterator i = offset.begin(); i != offset.end(); ++ i)
      *i = 0;

   // data channels
   map<Address, Transport*, AddrComp> DataChn;
   int reuseport = 0;
   set<int> fileid;

   while (true)
   {
      char speip[64];
      int speport;
      SectorMsg msg;
      int msgid;
      if (gmp->recvfrom(speip, speport, msgid, &msg) < 0)
         continue;

      // client releases the task
      if ((speip == client_ip) && (speport == client_port))
         break;

      int bucket = *(int32_t*)msg.getData();
      fileid.insert(bucket);

      char* tmp = new char[self->m_strHomeDir.length() + path.length() + localfile.length() + 64];
      sprintf(tmp, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), bucket);
      ofstream datafile(tmp, ios::app);
      sprintf(tmp, "%s.%d.idx", (self->m_strHomeDir + path + "/" + localfile).c_str(), bucket);
      ofstream indexfile(tmp, ios::app);
      delete [] tmp;
      int64_t start = offset[bucket];
      if (0 == start)
         indexfile.write((char*)&start, 8);

      Address n;
      n.m_strIP = speip;
      n.m_iPort = *(int32_t*)(msg.getData() + 4); // SPE ID

      Transport* chn = NULL;

      map<Address, Transport*, AddrComp>::iterator i = DataChn.find(n);
      if (i != DataChn.end())
      {
         chn = i->second;
         msg.m_iDataLength = SectorMsg::m_iHdrSize;

         // channel exists, no response to be sent; start receiving data
      }
      else
      {
         Transport* t = new Transport;
         int dataport;
         int remoteport = *(int32_t*)(msg.getData() + 8);
	 if (speip != self->m_strLocalHost)
         {
            dataport = reuseport;
            t->open(dataport, true, true);
            reuseport = dataport;
         }
         else
         {
            dataport = 0;
            t->open(dataport, true, false);
         }

         *(int32_t*)msg.getData() = dataport;
         msg.m_iDataLength = SectorMsg::m_iHdrSize + 4;
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

      datafile.close();
      indexfile.close();
   }

   gmp->close();
   delete gmp;

   // release data channels
   for (map<Address, Transport*, AddrComp>::iterator i = DataChn.begin(); i != DataChn.end(); ++ i)
   {
      i->second->close();
      delete i->second;
   }

   // report sphere output files
   for (set<int>::iterator i = fileid.begin(); i != fileid.end(); ++ i)
   {
      char* tmp = new char[path.length() + localfile.length() + 64];
      sprintf(tmp, "%s.%d", (path + "/" + localfile).c_str(), *i);
      self->report(0, tmp, 1);
      sprintf(tmp, "%s.%d.idx", (path + "/" + localfile).c_str(), *i);
      self->report(0, tmp, 1);
      delete [] tmp;
   }

   self->reportSphere(transid);

   return NULL;
}

int Slave::SPEReadData(const string& datafile, const int64_t& offset, int& size, int64_t* index, const int64_t& totalrows, char*& block)
{
   SNode sn;
   string idxfile = datafile + ".idx";

   //read index
   if (m_LocalFile.lookup(idxfile.c_str(), sn) > 0)
   {
      ifstream idx;
      idx.open((m_strHomeDir + idxfile).c_str());
      idx.seekg(offset * 8);
      idx.read((char*)index, (totalrows + 1) * 8);
      idx.close();
   }
   else
   {
      SectorMsg msg;
      msg.setType(110); // open the index file
      msg.setKey(0);

      Transport datachn;
      int port = 0;
      datachn.open(port, true, true);

      msg.setData(0, (char*)&port, 4);
      int32_t mode = 1;
      msg.setData(4, (char*)&mode, 4);
      msg.setData(8, idxfile.c_str(), idxfile.length() + 1);

      if (m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg) < 0)
         return -1;
      if (msg.getType() < 0)
         return -1;

      cout << "rendezvous connect " << msg.getData() << " " << *(int*)(msg.getData() + 68) << endl;
      if (datachn.connect(msg.getData(), *(int*)(msg.getData() + 68)) < 0)
         return -1;

      char req[20];
      *(int32_t*)req = 1;
      *(int64_t*)(req + 4) = offset * 8;
      *(int64_t*)(req + 12) = (totalrows + 1) * 8;
      int32_t response = -1;

      if (datachn.send(req, 20) < 0)
         return -1;
      if ((datachn.recv((char*)&response, 4) < 0) || (-1 == response))
         return -1;
      if (datachn.recv((char*)index, (totalrows + 1) * 8) < 0)
         return -1;

      int32_t cmd = 5;
      datachn.send((char*)&cmd, 4);
      datachn.recv((char*)&cmd, 4);
      datachn.close();
   }

   size = index[totalrows] - index[0];
   block = new char[size];

   // read data file
   if (m_LocalFile.lookup(datafile.c_str(), sn) > 0)
   {
      ifstream ifs;
      ifs.open((m_strHomeDir + datafile).c_str());
      ifs.seekg(index[0]);
      ifs.read(block, size);
      ifs.close();
   }
   else
   {
      SectorMsg msg;
      msg.setType(110); // open the file
      msg.setKey(0);

      Transport datachn;
      int port = 0;
      datachn.open(port, true, true);

      msg.setData(0, (char*)&port, 4);
      int32_t mode = 1;
      msg.setData(4, (char*)&mode, 4);
      msg.setData(8, datafile.c_str(), datafile.length() + 1);

      if (m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg) < 0)
         return -1;
      if (msg.getType() < 0)
         return -1;

      cout << "rendezvous connect " << msg.getData() << " " << *(int*)(msg.getData() + 68) << endl;
      if (datachn.connect(msg.getData(), *(int*)(msg.getData() + 68)) < 0)
         return -1;

      char req[20];
      *(int32_t*)req = 1; // cmd read
      *(int64_t*)(req + 4) = index[0];
      *(int64_t*)(req + 12) = index[totalrows] - index[0];
      int32_t response = -1;

      if (datachn.send(req, 20) < 0)
         return -1;
      if ((datachn.recv((char*)&response, 4) < 0) || (-1 == response))
         return -1;
      if (datachn.recv(block, index[totalrows] - index[0]) < 0)
         return -1;

      int32_t cmd = 5;
      datachn.send((char*)&cmd, 4);
      datachn.recv((char*)&cmd, 4);
      datachn.close();
   }

   return totalrows;
}

int Slave::SPESendResult(const int& speid, const int& buckets, const SPEResult& result, const string& localfile, Transport* datachn, char* locations, map<Address, Transport*, AddrComp>* outputchn)
{
   if (buckets == -1)
   {
      ofstream ofs;
      ofs.open((m_strHomeDir + localfile).c_str());
      ofs.write(result.m_vData[0], result.m_vDataLen[0]);
      ofs.close();
      ofs.open((m_strHomeDir + localfile + ".idx").c_str());
      ofs.write((char*)result.m_vIndex[0], result.m_vIndexLen[0] * 8);
      ofs.close();

      // report the result file to master
      report(0, localfile, true);

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
         if (sarray[i] > 0)
            rarray[i] = result.m_vIndexLen[i] - 1;
         else
            rarray[i] = 0;
      }

      // send back size and recnum information
      datachn->send((char*)sarray, buckets * 4);
      datachn->send((char*)rarray, buckets * 4);
      delete [] sarray;
      delete [] rarray;

      int reuseport = 0;

      for (int r = speid; r < buckets + speid; ++ r)
      {
         // start from a random location, to avoid writing to the same SPE shuffler, which lead to slow synchronization problem
         int i = r % buckets;

         if (0 == result.m_vDataLen[i])
            continue;

         char* dstip = locations + i * 72;
         //int32_t dstport = *(int32_t*)(locations + i * 72 + 64);
         int32_t shufflerport = *(int32_t*)(locations + i * 72 + 68);

         SectorMsg msg;
         msg.setData(0, (char*)&i, 4);
         msg.setData(4, (char*)&speid, 4);

         Transport* chn;

         Address n;
         n.m_strIP = dstip;
         n.m_iPort = shufflerport;

         map<Address, Transport*, AddrComp>::iterator c = outputchn->find(n);
         if (c != outputchn->end())
         {
            // channel exists, send a message immediately followed by data, no response expected
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
            int id = 0;
            m_GMP.sendto(dstip, shufflerport, id, &msg);

            chn = c->second;
         }
         else
         {
            Transport* t = new Transport;
            int dataport;
            if (dstip != m_strLocalHost)
            {
               dataport = reuseport;
               t->open(dataport, true, true);
               reuseport = dataport;
            }
            else
            {
               dataport = 0;
               t->open(dataport, true, false);
            }

            msg.setData(8, (char*)&dataport, 4);
            msg.m_iDataLength = SectorMsg::m_iHdrSize + 12;

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

   return 1;
}

int Slave::acceptLibrary(const int& key, Transport* datachn)
{
   int32_t size = -1;

   datachn->recv((char*)&size, 4);

   while (size > 0)
   {
      char* lib = new char[size];
      datachn->recv(lib, size);
      datachn->recv((char*)&size, 4);
      char* buf = new char[size];
      datachn->recv(buf, size);

      char* path = new char[m_strHomeDir.length() + 64];
      sprintf(path, "%s/.sphere/%d", m_strHomeDir.c_str(), key);

      struct stat s;
      if (stat((string(path) + "/" + lib).c_str(), &s) < 0)
      {
         ::mkdir(path, S_IRWXU);

         ofstream ofs((string(path) + "/" + lib).c_str(), ios::trunc);
         ofs.write(buf, size);
         ofs.close();

         system((string("chmod +x '") + path + "/" + lib + "'").c_str());
      }

      delete [] lib;
      delete [] buf;
      delete [] path;

      datachn->recv((char*)&size, 4);
   }

   return 0;
}
