/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __STORE_H__
#define __STORE_H__

#include <routing.h>
#include <gmp.h>
#include <transport.h>
#include <log.h>
#include <conf.h>
#include <index.h>
#include <kb.h>

namespace cb
{

class Server
{
public:
   Server(const string& ip);
   ~Server();

public:
   int init(char* ip = NULL, int port = 0);
   int run();

private:
   struct Param1
   {
      Server* serv_instance;	// self
      string client_ip;		// client IP
      int client_ctrl_port;	// client control port
      int32_t msg_id;		// message ID
      CCBMsg* msg;		// message
   };

   struct Param2
   {
      Server* serv_instance;	// self
      string filename;		// filename
      Transport* datachn;	// data channel
      int mode;			// file access mode
      string client_ip;		// client IP
      int client_data_port;	// client data channel port
   };

   struct Param3
   {
      Server* serv_instance;	// self
      string filename;        	// filename
      string query;		// query
      Transport* datachn;	// data channel
      string client_ip;	        // client IP
      int client_data_port;     // client UDT port
   };

   struct Param4
   {
      Server* serv_instance;	// self
      Transport* datachn;	// data channel
      string client_ip;		// client IP
      int client_ctrl_port;	// client GMP port
      int client_data_port;	// client data port
      int speid;		// speid
      string function;		// SPE operator
      char* param;		// SPE parameter
      int psize;		// parameter size
   };

   static void* process(void* s);
   static void* processEx(void* p1);
   static void* fileHandler(void* p2);
   static void* SQLHandler(void* p3);
   static void* SPEHandler(void* p4);

private:
   void updateOutLink();
   void updateInLink();

   int initLocalFile();
   void updateLocalFile();

private:
   CRouting m_Router;

   CGMP m_GMP;

   LocalFileIndex m_LocalFile;
   RemoteFileIndex m_RemoteFile;

   string m_strLocalHost;
   int m_iLocalPort;

   static const int m_iKeySpace = 32;

   string m_strHomeDir;

   CAccessLog m_AccessLog;
   CPerfLog m_PerfLog;

   SECTORParam m_SysConfig;

   KnowledgeBase m_KBase;
};

}; //namespace

#endif
