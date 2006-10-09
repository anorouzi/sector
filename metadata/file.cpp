#include <file.h>

using namespace std;

CFileAttr::CFileAttr() 
{
   m_pcName[0] = '\0';
   m_uiID = 0;
   gettimeofday(&m_TimeStamp, 0);
   memset(m_pcType, 0, 64);
   m_iAttr = 0;
   m_iIsDirectory = 0;
   m_llSize = 0;
   m_pcHost[0] = '\0';
   m_iPort = 0;
   m_pcNameHost[0] = '\0';
   m_iNamePort = 0;
}

CFileAttr::~CFileAttr()
{
}

CFileAttr& CFileAttr::operator=(const CFileAttr& f)
{
   strcpy(m_pcName, f.m_pcName);
   m_uiID = f.m_uiID;
   m_TimeStamp = f.m_TimeStamp;
   memcpy(m_pcType, f.m_pcType, 64);
   m_iAttr = f.m_iAttr;
   m_iIsDirectory = f.m_iIsDirectory;
   m_llSize = f.m_llSize;
   strcpy(m_pcHost, f.m_pcHost);
   m_iPort = f.m_iPort;
   strcpy(m_pcNameHost, f.m_pcNameHost);
   m_iNamePort = f.m_iNamePort;

   return *this;
}

void CFileAttr::synchronize(char* attr, int& len) const
{
   char* p = attr;

   memcpy(p, m_pcName, 64);
   p += 64;
   memcpy(p, m_pcType, 64);
   p += 64;
   memcpy(p, m_pcHost, 64);
   p += 64; 

   sprintf(p, "%d %ld %ld %d %lld %d \n", m_uiID, m_TimeStamp.tv_sec, m_TimeStamp.tv_usec, m_iIsDirectory, m_llSize, m_iPort);

   len = 64 * 3 + strlen(p) + 1;
}

void CFileAttr::desynchronize(const char* attr, const int& len)
{
   char* p = (char*)attr;

   memcpy(m_pcName, p, 64);
   p += 64;
   memcpy(m_pcType, p, 64);
   p += 64;
   memcpy(m_pcHost, p, 64);
   p += 64;

   sscanf(p, "%d %ld %ld %d %lld %d", &m_uiID, &m_TimeStamp.tv_sec, &m_TimeStamp.tv_usec, &m_iIsDirectory, &m_llSize, &m_iPort);
}
