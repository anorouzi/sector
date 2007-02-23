#ifndef __NODE_H__
#define __NODE_H__

struct Node
{
   uint32_t m_uiID;
   char m_pcIP[64];
   int32_t m_iPort;
   int32_t m_iAppPort;
};

#endif
