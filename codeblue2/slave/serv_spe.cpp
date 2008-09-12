/*****************************************************************************
Copyright � 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 09/11/2008
*****************************************************************************/

#include <slave.h>
#include <sphere.h>
#include <dlfcn.h>
#include <iostream>
#include <algorithm>

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

   m_llTotalDataSize = 0;
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
   m_llTotalDataSize += len;
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

   m_llTotalDataSize = 0;
}

SPEDestination::SPEDestination():
m_piSArray(NULL),
m_piRArray(NULL),
m_pcOutputLoc(NULL)
{
}

SPEDestination::~SPEDestination()
{
   delete [] m_piSArray;
   delete [] m_piRArray;
   delete [] m_pcOutputLoc;
}

void SPEDestination::init(const int& buckets)
{
   if (buckets > 0)
   {
      m_piSArray = new int[buckets];
      m_piRArray = new int[buckets];
      for (int i = 0; i < buckets; ++ i)
         m_piSArray[i] = m_piRArray[i] = 0;
   }
   else
   {
      m_piSArray = new int[1];
      m_piRArray = new int[1];
      m_piSArray[0] = m_piRArray[0] = 0;
   }
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
   const int type = ((Param4*)p)->type;
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

   SPEDestination dest;
   if (buckets > 0)
   {
      dest.m_pcOutputLoc = new char[buckets * 72];
      if (datachn->recv(dest.m_pcOutputLoc, buckets * 72) < 0)
         return NULL;
   }
   else if (buckets < 0)
   {
      int32_t size = 0;
      if (datachn->recv((char*)&size, 4) < 0)
         return NULL;

      dest.m_pcOutputLoc = new char[size];
      if (datachn->recv(dest.m_pcOutputLoc, size) < 0)
         return NULL;
      dest.m_strLocalFile = dest.m_pcOutputLoc;
   }
   dest.init(buckets);


   // initialize processing function
   self->acceptLibrary(key, datachn);
   SPHERE_PROCESS process = NULL;
   MR_MAP map = NULL;
   MR_PARTITION partition = NULL;
   void* lh = NULL;
   self->openLibrary(key, function, lh);
   if (NULL == lh)
      return NULL;

   if (type == 0)
      self->getSphereFunc(lh, function, process);
   else if (type == 1)
      self->getMapFunc(lh, function, map, partition);
   else
      return NULL;


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
      sprintf(dest.m_pcLocalFileID, ".%d", dsid);

      delete [] dataseg;
      cout << "new job " << datafile << " " << offset << " " << totalrows << endl;

      int64_t* index = NULL;
      if ((totalrows > 0) && (rows != 0))
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
      if (size < 64000000)
         size = 64000000;
      rdata = new char[size];

      int64_t* rindex = NULL;
      int* rbucket = NULL;
      int rowsbuf = totalrows + 2;
      if (rowsbuf < 640000)
         rowsbuf = 640000;
      rindex = new int64_t[rowsbuf];
      rbucket = new int[rowsbuf];

      SInput input;
      input.m_pcUnit = NULL;
      input.m_pcParam = (char*)param;
      input.m_iPSize = psize;
      SOutput output;
      output.m_pcResult = rdata;
      output.m_iBufSize = size;
      output.m_pllIndex = rindex;
      output.m_iIndSize = rowsbuf;
      output.m_piBucketID = rbucket;
      output.m_llOffset = 0;
      SFile file;
      file.m_strHomeDir = self->m_strHomeDir;
      char path[64];
      sprintf(path, "%d", key);
      file.m_strLibDir = self->m_strHomeDir + ".sphere/" + path + "/";
      file.m_strTempDir = self->m_strHomeDir + ".tmp/";

      result.clear();
      gettimeofday(&t3, 0);

      // process data segments
      for (int i = 0; i < totalrows; i += unitrows)
      {
         if (unitrows > totalrows - i)
            unitrows = totalrows - i;

         input.m_pcUnit = block + index[i] - index[0];
         input.m_iRows = unitrows;
         input.m_pllIndex = index + i;

         self->processData(input, output, file, result, process, map, partition);

         if ((result.m_llTotalDataSize > 16000000) && (buckets != 0))
            self->deliverResult(buckets, speid, result, dest);

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

      // process files
      if (0 == unitrows)
      {
         input.m_pcUnit = block;
         input.m_iRows = -1;
         input.m_pllIndex = NULL;

         while (output.m_llOffset >= 0)
         {
            self->processData(input, output, file, result, process, map, partition);

            if ((result.m_llTotalDataSize > 16000000) && (buckets != 0))
               self->deliverResult(buckets, speid, result, dest);
         }
      }

      self->deliverResult(buckets, speid, result, dest);

      cout << "completed 100 " << ip << " " << ctrlport << endl;
      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
      int id = 0;
      self->m_GMP.sendto(ip.c_str(), ctrlport, id, &msg);

      cout << "sending data back... " << buckets << endl;
      self->sendResultToClient(buckets, dest.m_piSArray, dest.m_piRArray, result, datachn);

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

   self->closeLibrary(lh);
   datachn->close();
   delete datachn;

   cout << "comp server closed " << ip << " " << ctrlport << " " << duration << endl;

   delete [] param;

   for (std::map<Address, Transport*, AddrComp>::iterator i = dest.m_mOutputChn.begin(); i != dest.m_mOutputChn.end(); ++ i)
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
   int bucketnum = ((Param5*)p)->bucketnum;
   int bucketid = ((Param5*)p)->bucketid;
   CGMP* gmp = ((Param5*)p)->gmp;
   const int key = ((Param5*)p)->key;
   const int type = ((Param5*)p)->type;
   string function = ((Param5*)p)->function;
   delete (Param5*)p;

   cout << "SPE Shuffler " << path << " " << localfile << " " << bucketnum << endl;

   self->createDir(path);

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

   // sort and reduce
   if (type == 1)
   {
      void* lh = NULL;
      self->openLibrary(key, function, lh);
      //if (NULL == lh)
      //   break;

      MR_COMPARE comp = NULL;
      MR_REDUCE reduce = NULL;
      self->getReduceFunc(lh, function, comp, reduce);

      for (set<int>::iterator i = fileid.begin(); i != fileid.end(); ++ i)
      {
         char* tmp = new char[self->m_strHomeDir.length() + path.length() + localfile.length() + 64];
         sprintf(tmp, "%s.%d", (self->m_strHomeDir + path + "/" + localfile).c_str(), *i);

         if (comp != NULL)
         {
            self->sort(tmp, comp, reduce);
         }
      }

      self->closeLibrary(lh);
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

   cout << "bucket completed 100 " << client_ip << " " << client_port << endl;
   SectorMsg msg;
   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(bucketid), 4);
   int progress = 100;
   msg.setData(4, (char*)&progress, 4);
   msg.m_iDataLength = SectorMsg::m_iHdrSize + 8;
   int id = 0;
   self->m_GMP.sendto(client_ip.c_str(), client_port, id, &msg);

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

int Slave::sendResultToFile(const SPEResult& result, const string& localfile, const int64_t& offset)
{
   ofstream datafile, idxfile;
   datafile.open((m_strHomeDir + localfile).c_str(), ios::app);
   idxfile.open((m_strHomeDir + localfile + ".idx").c_str(), ios::app);

   datafile.write(result.m_vData[0], result.m_vDataLen[0]);

   if (offset == 0)
      idxfile.write((char*)&offset, 8);
   else
   {
      for (int i = 1; i <= result.m_vIndexLen[0]; ++ i)
         result.m_vIndex[0][i] += offset;
   }
   idxfile.write((char*)(result.m_vIndex[0] + 1), (result.m_vIndexLen[0] - 1) * 8);

   datafile.close();
   idxfile.close();

   return 0;
}

int Slave::sendResultToClient(const int& buckets, const int* sarray, const int* rarray, const SPEResult& result, Transport* datachn)
{
   if (buckets == -1)
   {
      // send back result file/record size
      datachn->send((char*)sarray, 4);
      datachn->send((char*)rarray, 4);
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
      // send back size and recnum information
      datachn->send((char*)sarray, buckets * 4);
      datachn->send((char*)rarray, buckets * 4);
   }

   return 0;
}

int Slave::sendResultToBuckets(const int& speid, const int& buckets, const SPEResult& result, char* locations, map<Address, Transport*, AddrComp>* outputchn)
{
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

         system((string("chmod +x ") + reviseSysCmdPath(path) + "/" + reviseSysCmdPath(lib)).c_str());
      }

      delete [] lib;
      delete [] buf;
      delete [] path;

      datachn->recv((char*)&size, 4);
   }

   return 0;
}

int Slave::openLibrary(const int& key, const string& lib, void*& lh)
{
   char path[64];
   sprintf(path, "%d", key);
   lh = dlopen((m_strHomeDir + ".sphere/" + path + "/" + lib + ".so").c_str(), RTLD_LAZY);
   if (NULL == lh)
   {
      cerr << dlerror() << endl;
      return -1;
   }

   return 0;
}

int Slave::getSphereFunc(void* lh, const string& function, SPHERE_PROCESS& process)
{
   process = (SPHERE_PROCESS)dlsym(lh, function.c_str());
   if (NULL == process)
   {
      cerr << dlerror() << endl;
      return -1;
   }

   return 0;
}

int Slave::getMapFunc(void* lh, const string& function, MR_MAP& map, MR_PARTITION& partition)
{
   map = (MR_MAP)dlsym(lh, (function + "_map").c_str());
   if (NULL == map)
      cerr << dlerror() << endl;

   partition = (MR_PARTITION)dlsym(lh, (function + "_partition").c_str());
   if (NULL == partition)
   {
      cerr << dlerror() << endl;
      return -1;
   }

   return 0;
}

int Slave::getReduceFunc(void* lh, const string& function, MR_COMPARE& compare, MR_REDUCE& reduce)
{
   reduce = (MR_REDUCE)dlsym(lh, (function + "_reduce").c_str());
   if (NULL == reduce)
      cerr << dlerror() << endl;

   compare = (MR_COMPARE)dlsym(lh, (function + "_compare").c_str());
   if (NULL == compare)
   {
      cerr << dlerror() << endl;
      return -1;
   }

   return 0;

}

int Slave::closeLibrary(void* lh)
{
   return dlclose(lh);
}

int Slave::sort(const string& bucket, MR_COMPARE comp, MR_REDUCE red)
{
   ifstream ifs(bucket.c_str());
   if (ifs.fail())
      return -1;

   ifs.seekg(0, ios::end);
   int size = ifs.tellg();
   ifs.seekg(0, ios::beg);
   char* rec = new char[size];
   ifs.read(rec, size);
   ifs.close();

   ifs.open((bucket + ".idx").c_str());
   if (ifs.fail())
   {
      delete [] rec;
      return -1;
   }

   ifs.seekg(0, ios::end);
   size = ifs.tellg();
   ifs.seekg(0, ios::beg);
   int64_t* idx = new int64_t[size / 8];
   ifs.read((char*)idx, size);
   ifs.close();

   size = size / 8 - 1;

   vector<MRRecord> vr;
   vr.resize(size);
   int64_t offset = 0;
   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end(); ++ i)
   {
      i->m_pcData = rec + idx[offset];
      i->m_iSize = idx[offset + 1] - idx[offset];
      i->m_pCompRoutine = comp;
      offset ++;
   }

   //std::sort(vr.begin(), vr.end(), ltrec());
   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end(); ++ i)
   {
      for (vector<MRRecord>::iterator j = i; j != vr.end(); ++ j)
      {
         ltrec comp;
         if (!comp(*i, *j))
         {
            MRRecord tmp = *i;
            *i = *j;
            *j = tmp;
         }
      }
   }

   if (red != NULL)
      reduce(vr, bucket, red, NULL, 0);

   ofstream sorted((bucket + ".sorted").c_str(), ios::trunc);
   ofstream sortedidx((bucket + ".sorted.idx").c_str(), ios::trunc);
   offset = 0;
   sortedidx.write((char*)&offset, 8);
   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end(); ++ i)
   {
      sorted.write(i->m_pcData, i->m_iSize);
      offset += i->m_iSize;
      sortedidx.write((char*)&offset, 8);
   }
   sorted.close();
   sortedidx.close();

   delete [] rec;
   delete [] idx;

   return 0;
}

int Slave::reduce(vector<MRRecord>& vr, const string& bucket, MR_REDUCE red, void* param, int psize)
{
   SInput input;
   input.m_pcUnit = NULL;
   input.m_pcParam = (char*)param;
   input.m_iPSize = psize;


   int rdsize = 256000000;
   int risize = 1000000;

   SOutput output;
   output.m_pcResult = new char[rdsize];
   output.m_iBufSize = rdsize;
   output.m_pllIndex = new int64_t[risize];
   output.m_iIndSize = risize;
   output.m_piBucketID = new int[risize];
   output.m_llOffset = 0;

   SFile file;
   file.m_strHomeDir = m_strHomeDir;
//   file.m_strLibDir = m_strHomeDir + ".sphere/" + path + "/";
   file.m_strTempDir = m_strHomeDir + ".tmp/";

   char* idata = new char[256000000];
   int64_t* iidx = new int64_t[1000000];

   ofstream reduced((bucket + ".reduced").c_str(), ios::trunc);
   ofstream reducedidx((bucket + ".reduced.idx").c_str(), ios::trunc);
   int64_t roff = 0;

   for (vector<MRRecord>::iterator i = vr.begin(); i != vr.end();)
   {
      iidx[0] = 0;
      vector<MRRecord>::iterator curr = i;
      memcpy(idata, i->m_pcData, i->m_iSize);
      iidx[1] = i->m_iSize;
      int offset = 1;

      i ++;
      while ((i != vr.end()) && (i->m_pCompRoutine(curr->m_pcData, curr->m_iSize, i->m_pcData, i->m_iSize) == 0))
      {
         memcpy(idata + iidx[offset], i->m_pcData, i->m_iSize);
         iidx[offset + 1] = iidx[offset] + i->m_iSize;
         offset ++;
         i ++;
      }

      input.m_pcUnit = idata;
      input.m_pllIndex = iidx;
      input.m_iRows = offset;
      red(&input, &output, &file);

      for (int r = 0; r < output.m_iRows; ++ r)
      {
         cout << "RES: " << output.m_pcResult + output.m_pllIndex[r] << endl;
         reduced.write(output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
         roff += output.m_pllIndex[r + 1] - output.m_pllIndex[r];
         reducedidx.write((char*)&roff, 8);
      }
   }

   reduced.close();
   reducedidx.close();

   delete [] output.m_pcResult;
   delete [] output.m_pllIndex;
   delete [] output.m_piBucketID;
   delete [] idata;
   delete [] iidx;

   return 0;
}

int Slave::processData(SInput& input, SOutput& output, SFile& file, SPEResult& result, SPHERE_PROCESS process, MR_MAP map, MR_PARTITION partition)
{
   // pass relative offset, from 0, to the processing function
   int64_t uoff = (input.m_pllIndex != NULL) ? input.m_pllIndex[0] : 0;
   for (int p = 0; p <= input.m_iRows; ++ p)
      input.m_pllIndex[p] = input.m_pllIndex[p] - uoff;

   if (NULL != process)
   {
      process(&input, &output, &file);
      for (int r = 0; r < output.m_iRows; ++ r)
         result.addData(output.m_piBucketID[r], output.m_pcResult + output.m_pllIndex[r], output.m_pllIndex[r + 1] - output.m_pllIndex[r]);
   }
   else
   {
      if (NULL == map)
      {
         // partition input directly if there is no map
         for (int r = 0; r < input.m_iRows; ++ r)
         {
            char* data = input.m_pcUnit + input.m_pllIndex[r];
            int size = input.m_pllIndex[r + 1] - input.m_pllIndex[r];
            result.addData(partition(data, size, input.m_pcParam, input.m_iPSize), data, size);
         }
      }
      else
      {
         map(&input, &output, &file);
         for (int r = 0; r < output.m_iRows; ++ r)
         {
            char* data = output.m_pcResult + output.m_pllIndex[r];
            int size = output.m_pllIndex[r + 1] - output.m_pllIndex[r];
            result.addData(partition(data, size, input.m_pcParam, input.m_iPSize), data, size);
	 }
      }
   }

   // restore the original offset
   for (int p = 0; p <= input.m_iRows; ++ p)
      input.m_pllIndex[p] = input.m_pllIndex[p] + uoff;

   return 0;
}

int Slave::deliverResult(const int& buckets, const int& speid, SPEResult& result, SPEDestination& dest)
{
   if (buckets == -1)
      sendResultToFile(result, dest.m_strLocalFile + dest.m_pcLocalFileID, dest.m_piSArray[0]);
   else
      sendResultToBuckets(speid, buckets, result, dest.m_pcOutputLoc, &dest.m_mOutputChn);

   for (int b = 0; b < buckets; ++ b)
   {
      dest.m_piSArray[b] += result.m_vDataLen[b];
      if (result.m_vDataLen[b] > 0)
         dest.m_piRArray[b] += result.m_vIndexLen[b] - 1;
   }

   result.clear();

   return 0;
}
