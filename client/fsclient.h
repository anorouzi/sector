#ifndef __CBFS_CLIENT_H__
#define __CBFS_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <file.h>
#include <node.h>
#include <client.h>
#include <udt.h>

class CCBFile;

class CFSClient: public Client
{
friend class CCBFile;

public:
   CCBFile* createFileHandle();
   void releaseFileHandle(CCBFile* f);

   int ls(vector<CIndexInfo>& filelist);
   int stat(const string& filename, CFileAttr& attr);
};

class CCBFile
{
friend class CFSClient;

private:
   CCBFile();
   virtual ~CCBFile();

public:
   int open(const string& filename, const int& mode = 3, char* cert = NULL);
   int read(char* buf, const int64_t& offset, const int64_t& size);
   int write(const char* buf, const int64_t& offset, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

private:
   CFSClient* m_pFSClient;

   string m_strServerIP;
   int m_iServerPort;

   CGMP m_GMP;

   string m_strFileName;

   int m_iProtocol;     // 1 UDT 2 TCP

   UDTSOCKET m_uSock;
   int m_tSock;
};

#endif
