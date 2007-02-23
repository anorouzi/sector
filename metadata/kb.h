#ifndef __KNOWLEDGE_BASE_H__
#define __KNOWLEDGE_BASE_H__

class CKnowledgeBase
{
public:
   CKnowledgeBase():
   m_iNumConn(0)
   {
   }

public:
   int init();
   int refresh();

public:
   int m_iNumConn;

   int m_iCPUIndex;
   int m_iMemSize;

   int m_iDiskReadIndex;
   int m_iDiskWriteIndex;
};

#endif
