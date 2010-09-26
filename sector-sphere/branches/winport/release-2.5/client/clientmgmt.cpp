/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 03/16/2010
*****************************************************************************/

#include <sector.h>
#include <common.h>
#include "clientmgmt.h"

using namespace std;

ClientMgmt::ClientMgmt():
m_iID(0)
{





}

ClientMgmt::~ClientMgmt()
{





}

Client* ClientMgmt::lookupClient(const int& id)
{
    Client* c = NULL;

    m_CLock.acquire();

    map<int, Client*>::iterator i = m_mClients.find(id);
    if (i != m_mClients.end())
        c = i->second;

    m_CLock.release();

    return c;
}

FSClient* ClientMgmt::lookupFS(const int& id)
{
    FSClient* f = NULL;

    m_FSLock.acquire();

    map<int, FSClient*>::iterator i = m_mSectorFiles.find(id);
    if (i != m_mSectorFiles.end())
        f = i->second;

    m_FSLock.release();

    return f;
}

DCClient* ClientMgmt::lookupDC(const int& id)
{
    DCClient* d = NULL;

    m_DCLock.acquire();

    map<int, DCClient*>::iterator i = m_mSphereProcesses.find(id);
    if (i != m_mSphereProcesses.end())
        d = i->second;

    m_DCLock.release();
    
    return d;
}

int ClientMgmt::insertClient(Client* c)
{
    m_CLock.acquire();
    
    int id = g_ClientMgmt.m_iID ++;
    g_ClientMgmt.m_mClients[id] = c;

    m_CLock.release();

    return id;
}

int ClientMgmt::insertFS(FSClient* f)
{
    m_CLock.acquire();
    int id = g_ClientMgmt.m_iID ++;
    m_CLock.release();

    m_FSLock.acquire();
    g_ClientMgmt.m_mSectorFiles[id] = f;
    m_FSLock.release();

    return id;
}

int ClientMgmt::insertDC(DCClient* d)
{
    m_CLock.acquire();
    int id = g_ClientMgmt.m_iID ++;
    m_CLock.release();

    m_DCLock.acquire();
    g_ClientMgmt.m_mSphereProcesses[id] = d;
    m_DCLock.release();

    return id;
}

int ClientMgmt::removeClient(const int& id)
{
    CGuard guard(m_CLock);
    
    g_ClientMgmt.m_mClients.erase(id);

    return 0;
}

int ClientMgmt::removeFS(const int& id)
{
    CGuard guard(m_FSLock);

    g_ClientMgmt.m_mSectorFiles.erase(id);

    return 0;
}

int ClientMgmt::removeDC(const int& id)
{
    CGuard guard(m_DCLock);
    
    g_ClientMgmt.m_mSphereProcesses.erase(id);

    return 0;
}


int Sector::init(const string& server, const int& port)
{
   Client* c = new Client;
   int r = c->init(server, port);

   if (r >= 0)
   {
      m_iID = g_ClientMgmt.insertClient(c);
   }
   else
   {
      delete c;
   }

   return r;
}

int Sector::login(const string& username, const string& password, const char* cert)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->login(username, password, cert);
}

int Sector::logout()
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->logout();
}

int Sector::close()
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   g_ClientMgmt.removeClient(m_iID);

   c->close();
   delete c;

   return 0;
}

int Sector::list(const string& path, vector<SNode>& attr)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->list(path, attr);
}

int Sector::stat(const string& path, SNode& attr)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->stat(path, attr);
}

int Sector::mkdir(const string& path)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->mkdir(path);
}

int Sector::move(const string& oldpath, const string& newpath)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->move(oldpath, newpath);
}

int Sector::remove(const string& path)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->remove(path);
}

int Sector::rmr(const string& path)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->rmr(path);
}

int Sector::copy(const string& src, const string& dst)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->copy(src, dst);
}

int Sector::utime(const string& path, const int64_t& ts)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->utime(path, ts);
}

int Sector::sysinfo(SysStat& sys)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->sysinfo(sys);
}

int Sector::shutdown(const int& type, const string& param)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->shutdown(type, param);
}

int Sector::fsck(const string& path)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->fsck(path);
}

int Sector::setMaxCacheSize(const int64_t& ms)
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return SectorError::E_INVALID;

   return c->setMaxCacheSize(ms);
}

SectorFile* Sector::createSectorFile()
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return NULL;

   FSClient* f = c->createFSClient();
   SectorFile* sf = new SectorFile;

   sf->m_iID = g_ClientMgmt.insertFS(f);

   return sf;
}

SphereProcess* Sector::createSphereProcess()
{
   Client* c = g_ClientMgmt.lookupClient(m_iID);

   if (NULL == c)
      return NULL;

   DCClient* d = c->createDCClient();
   SphereProcess* sp = new SphereProcess;

   sp->m_iID = g_ClientMgmt.insertDC(d);

   return sp;
}

int Sector::releaseSectorFile(SectorFile* sf)
{
   if (NULL == sf)
      return 0;

   Client* c = g_ClientMgmt.lookupClient(m_iID);
   FSClient* f = g_ClientMgmt.lookupFS(sf->m_iID);

   if ((NULL == c) || (NULL == f))
      return SectorError::E_INVALID;

   g_ClientMgmt.removeFS(sf->m_iID);
   c->releaseFSClient(f);
   delete sf;
   return 0;
}

int Sector::releaseSphereProcess(SphereProcess* sp)
{
   if (NULL == sp)
      return 0;

   Client* c = g_ClientMgmt.lookupClient(m_iID);
   DCClient* d = g_ClientMgmt.lookupDC(sp->m_iID);

   if ((NULL == c) || (NULL == d))
      return SectorError::E_INVALID;

   g_ClientMgmt.removeDC(sp->m_iID);
   c->releaseDCClient(d);
   delete sp;
   return 0;
}

int SectorFile::open(const string& filename, int mode, const string& hint, const int64_t& reserve)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->open(filename, mode, hint, reserve);
}

int64_t SectorFile::read(char* buf, const int64_t& offset, const int64_t& size, const int64_t& prefetch)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->read(buf, offset, size, prefetch);
}

int64_t SectorFile::write(const char* buf, const int64_t& offset, const int64_t& size, const int64_t& buffer)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->write(buf, offset, size, buffer);
}

int64_t SectorFile::read(char* buf, const int64_t& size)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->read(buf, size);
}

int64_t SectorFile::write(const char* buf, const int64_t& size)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->write(buf, size);
}

int64_t SectorFile::download(const char* localpath, const bool& cont)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->download(localpath, cont);
}

int64_t SectorFile::upload(const char* localpath, const bool& cont)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->upload(localpath, cont);
}

int SectorFile::flush()
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->flush();
}

int SectorFile::close()
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->close();
}

int64_t SectorFile::seekp(int64_t off, int pos)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->seekp(off, pos);
}

int64_t SectorFile::seekg(int64_t off, int pos)
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->seekg(off, pos);
}

int64_t SectorFile::tellp()
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->tellp();
}

int64_t SectorFile::tellg()
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return SectorError::E_INVALID;

   return f->tellg();
}

bool SectorFile::eof()
{
   FSClient* f = g_ClientMgmt.lookupFS(m_iID);

   if (NULL == f)
      return true;

   return f->eof();
}

int SphereProcess::close()
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->close();
}

int SphereProcess::loadOperator(const char* library)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->loadOperator(library);
}

int SphereProcess::run(const SphereStream& input, SphereStream& output, const string& op, const int& rows, const char* param, const int& size, const int& type)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->run(input, output, op, rows, param, size, type);
}

int SphereProcess::run_mr(const SphereStream& input, SphereStream& output, const string& mr, const int& rows, const char* param, const int& size)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->run_mr(input, output, mr, rows, param, size);
}

int SphereProcess::read(SphereResult*& res, const bool& inorder, const bool& wait)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->read(res, inorder, wait);
}

int SphereProcess::checkProgress()
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->checkProgress();
}

int SphereProcess::checkMapProgress()
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->checkMapProgress();
}

int SphereProcess::checkReduceProgress()
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->checkReduceProgress();
}

int SphereProcess::waitForCompletion()
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL == d)
      return SectorError::E_INVALID;

   return d->waitForCompletion();
}

void SphereProcess::setMinUnitSize(int size)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL != d)
      d->setMinUnitSize(size);
}

void SphereProcess::setMaxUnitSize(int size)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL != d)
      d->setMaxUnitSize(size);
}

void SphereProcess::setProcNumPerNode(int num)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL != d)
      d->setProcNumPerNode(num);
}

void SphereProcess::setDataMoveAttr(bool move)
{
   DCClient* d = g_ClientMgmt.lookupDC(m_iID);

   if (NULL != d)
      d->setDataMoveAttr(move);
}
