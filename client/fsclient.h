#ifndef __CBFS_CLIENT_H__
#define __CBFS_CLIENT_H__

#include <routing.h>
#include <gmp.h>
#include <index.h>
#include <file.h>

class CCBFile;

class CFSClient
{
friend class CCBFile;

public:
   CFSClient();
   CFSClient(const int& protocol);
   ~CFSClient();

public:
   int connect(const string& server, const int& port);
   int close();

public:
   CCBFile* createFileHandle();
   void releaseFileHandle(CCBFile* f);

   int stat(const string& filename, CFileAttr& attr);
   int ls(vector<string>& filelist);

private:
   int lookup(string filename, Node* n);

private:
   string m_strServerHost;
   int m_iServerPort;

   CGMP* m_pGMP;

   int m_iProtocol;	// 1 UDT 2 TCP
};

class CCBFile
{
friend class CFSClient;

private:
   CCBFile();
   virtual ~CCBFile();

public:
   int open(const string& filename, const int& mode = 3);
   int read(char* buf, const int64_t& offset, const int64_t& size);
   int write(const char* buf, const int64_t& offset, const int64_t& size);
   int copy(const char* localpath, const bool& cont = false);
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
