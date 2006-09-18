#ifndef __CB_MESSAGE_H__
#define __CB_MESSAGE_H__

#include <sys/types.h>
#include <iostream>
using namespace std;

class CUserMessage
{
friend class CGMP;

public:
   CUserMessage();
   CUserMessage(const int& len);
   CUserMessage(const CUserMessage& msg);
   virtual ~CUserMessage();

public:
   int resize(const int& len);

public:
   char* m_pcBuffer;
   int m_iDataLength;
   int m_iBufLength;
};

class CRTMsg: public CUserMessage
{
public:
   int32_t getType() const;
   void setType(const int32_t& type);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 4;
};

class CCBMsg: public CUserMessage
{
public:
   int32_t getType() const;
   void setType(const int32_t& type);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 4;
};


#endif
