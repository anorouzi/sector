#ifndef __FILE_H__
#define __FILE_H__

#ifndef WIN32
   #include <stdint.h>
#endif
#include <util.h>
#include <string>
#include <set>

class CFSClient;

class CFileAttr
{
public:
   CFileAttr();
   virtual ~CFileAttr();

   CFileAttr& operator=(const CFileAttr& f);

public:
   void serialize(char* attr, int& len) const;
   void deserialize(const char* attr, const int& len);

public:
   char m_pcName[64];           // unique file name
   uint32_t m_uiID;	        // id
   int64_t m_llTimeStamp;       // time stamp
   char m_pcType[64];           // file type, data, video, audio, etc
   int32_t m_iAttr;	        // 01: READ	10: WRITE	11: READ&WRITE

   int32_t m_iIsDirectory;	// directory?

   int64_t m_llSize;		// size

   char m_pcHost[64];		// loc ip
   int32_t m_iPort;		// loc port

   char m_pcNameHost[64];	// ip for name server
   int32_t m_iNamePort;		// port for name server
};

struct CAttrComp
{
   bool operator()(const CFileAttr& a1, const CFileAttr& a2) const
   {
      int nc = strcmp(a1.m_pcName, a2.m_pcName);
      if (nc != 0)
          return (nc > 0);

      int hc = strcmp(a1.m_pcHost, a2.m_pcHost);
      if (hc != 0)
          return (hc > 0);

      return (a1.m_iPort > a2.m_iPort);
   }
};

#endif
