#include<cstdlib>
//#include<conio.c>
//#include<kbhit.c>
#include<fstream>
#include<fsclient.h>
#include<fcntl.h>
#include<stdio.h>
#include<unistd.h>
#include<sys/ioctl.h>
#include<termios.h>
#include<errno.h>
#include<iostream>
#include<iomanip>
#include<string>
#include<termios.h>
#include<vector>
#include<algorithm>

#include<curses.h>


using namespace std;


void parse(string &str, vector<string>& results, const string& delim=" ")
{
  unsigned int cutAt;
  while((cutAt=str.find_first_of(delim))!=str.npos)
    {
      if(cutAt>0)
	{
	  
	  results.push_back(str.substr(0,cutAt));
	}
      str=str.substr(cutAt+1);
    }
  if(str.length()>0)
	{
	  results.push_back(str);
	}
}

//this function list the directories and files.

int ls(vector<string> command, string cur_dir)
{
  if(command.size()>2)
    {
      cout<<"USAGE: ls <dir>"<<endl;
      return -1;
    }
  vector<SNode> filelist;
  if(command.size()==2)
    Sector::list(command.at(1),filelist);
  else
    Sector::list(cur_dir,filelist);
  
  for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
     {
      cout << setiosflags(ios::left) << setw(40) << i->m_strName << "\t";
      if (i->m_bIsDir)
	cout << "<dir>" << endl;
      else
	{
	time_t t = i->m_llTimeStamp;
         cout << i->m_llSize << " bytes " << "\t" << ctime(&t);
	}
     }
   return 1;
   
}
//put this function upload files & directories to the server

int put(vector<string> command)
{
  char* file;
  char* dst;

  if ((command.size()!=2)&&(command.size()!=3))
    {
      cout<<"USAGE: put <src file> [dst file]"<<endl;
      return -1;
    }
  else if(command.size()==2)
    {
      file=(char*)command.at(1).c_str();
      dst=NULL;
    }
  else 
    {
      file=(char*)command.at(1).c_str();
      dst=(char*)command.at(2).c_str();
    }
  timeval t1,t2;
  gettimeofday(&t1,0);

  ifstream ifs(file);
  ifs.seekg(0,ios::end);
  long long int size=ifs.tellg();
  ifs.seekg(0);
  cout<<"uploading "<<file<<" of "<<size<<" bytes"<<endl;
  
  SectorFile f;
  char* rname;
  if(NULL!=dst)
    {
      rname=dst; 
    }
  else
    {
      rname=file;
      for(int i=strlen(file);i>=0;--i)
	{
	  if('/'==file[i])
	    {
	      rname=file+i+1;
	      break;
	    }

	}
    }
  if(f.open(rname,2)<0)
    {
      cout<<"ERROR: unable to connect to server or file already exists."<<endl;
      return -1;
    }
  bool finish=true;
  if(f.upload(file)<0)
    finish=false;
  f.close();
  
  if(finish)
    {
      gettimeofday(&t2, 0);
      float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);
      
      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
    }
  else
    cout << "Uploading failed! Please retry. " << endl << endl;
  
  return 1;
  
  
}


int download(const char* file, const char* dest)
{
#ifndef WIN32
      timeval t1, t2;
#else
      DWORD t1, t2;
#endif
      
   #ifndef WIN32
      gettimeofday(&t1, 0);
#else
      t1 = GetTickCount();
#endif
      
      SNode attr;
   if (Sector::stat(file, attr) < 0)
     {
     cout << "ERROR: cannot locate file " << file << endl;
      return -1;
   }
   
   long long int size = attr.m_llSize;
   cout << "downloading " << file << " of " << size << " bytes" << endl;
   
   SectorFile f;
   
   if (f.open(file) < 0)
     {
     cout << "unable to locate file" << endl;
      return -1;
   }
   
   int sn = strlen(file) - 1;
   for (; sn >= 0; sn --)
   {
     if (file[sn] == '/')
	break;
   }
   string localpath;
   if (dest[strlen(dest) - 1] != '/')
     localpath = string(dest) + string("/") + string(file + sn + 1);
   else
     localpath = string(dest) + string(file + sn + 1);
   

   
   bool finish = true;
   if (f.download(localpath.c_str(), true) < 0)
     finish = false;

   f.close();
   
   if (finish)
     {
#ifndef WIN32
         gettimeofday(&t2, 0);
         float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);
#else
         float throughput = size * 8.0 / 1000000.0 / ((GetTickCount() - t1) / 1000.0);
#endif
	 
	 cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

      return 1;
   }
   
   return -1;
}


int get(vector<string> command)
{
  if(command.size()!=3)
    {
      cout<<"USAGE: get <srcfile> <local dir>"<<endl;
      return -1;
    }
  
  int c=0;
  for(;c<5;++c)
    {
      if (download(command.at(1).c_str(),command.at(2).c_str())>0)
	break;
      else if(c<4)
	cout<<"download interrupted, trying again from break point."<<endl;
      else
	cout<<"download failed after 5 attempts"<<endl;
    }
  return 1;
}

int  mkdir(vector<string> command,string cur_dir)
{
  if(command.size()!=2)
    {
      cout<<"USAGE: mkdir <dir>"<<endl;
      return -1;
    }
  if(Sector::mkdir(command.at(1).c_str())<0)
    {
      cout<<"fail to create the new directory"<<endl;
      return -1;
    }
  else
    {
      cout<<"directory "<<command.at(1)<<" created!"<<endl;
      return 1;
    }
}


int rm(vector<string> command)
{
  if(command.size()!=2)
    {
      cout<<"USAGE:rm <file path>"<<endl;
      return -1;
    }
  SNode attr;
  if(Sector::stat(command.at(1),attr)<0)
    {
      cout<<"ERROR: cannot locate file"<<endl;
      return -1;
    }
  if(Sector::remove(command.at(1))<0)
    {
      cout<<"cannot remove file"<<endl;
      return -1;
    }
  else
    cout<<"done"<<endl;
  
  return 1;
}

int mv(vector<string> command)
{
  if(command.size()!=3)
    {
      cout<<"USAGE:mv <src> <dst>"<<endl;
      return -1;
    }
  SNode attr;
  if(Sector::stat(command.at(1),attr)<0)
    {
      cout<<"ERROR: cannot locate source file:"<<command.at(1)<<endl;
      return -1;
    }
  
  cout<<"moving "<<command.at(1)<<" to"<<command.at(2)<<endl;
  
  if(Sector::move(command.at(1),command.at(2))<0 )
    {
      cout<<"ERROR: moving file failed."<<endl;
      return -1;
    }
  cout<< "moving "<<command.at(1)<<" to"<<command.at(2)<<" completed!"<<endl;
  long long int size=attr.m_llSize;
  cout<<"totally "<<size<<" bytes have been moved"<<endl;
  
  return 1;
}

int cd(vector<string> command, string& cur_dir)
{
  if(command.size()!=2)
    {
      cout<<"USAGE:cd <dir>"<<endl;
      return -1;
    }

  
  string old_dir;
  vector<SNode> filelist;
  if(command.at(1).c_str()[0]!='.'&& command.at(1).c_str()[0]!='/')// enter sub-directory of current director
    {
      
      old_dir=cur_dir;
      cur_dir=cur_dir+command.at(1);
      
      int a= Sector::list(cur_dir,filelist);
      if(a<=0) 
	{
	  cur_dir=old_dir;
	  cout<<"no such directory"<<endl;
	}
    }
  else if(command.at(1).c_str()[0]=='/')
    {
      old_dir=cur_dir;
      cur_dir=command.at(1);
      
      int a= Sector::list(cur_dir,filelist);
      if(a<=0) 
	{
	  cur_dir=old_dir;
	  cout<<"no such directory"<<endl;
	}
      
    }
  else if(command.at(1).c_str()[1]=='.')
    {
      
      old_dir=cur_dir;
      int pos1=old_dir.find_last_of("/");
      int pos2=command.at(1).find_first_of("/");
      string file_dir;
      if(pos2>=0)
	{
	  file_dir=command.at(1).substr(pos2+1);
	}
      else
	{
	  file_dir="";
	}
	  
      
      string upper_dir=old_dir.substr(0,pos1+1);
      cur_dir=upper_dir+file_dir;
      
      if(cur_dir=="/") return 1;

      int a= Sector::list(cur_dir,filelist);
      if(a<=0) 
	{
	  cur_dir=old_dir;
	  cout<<"no such directory"<<endl;
	} 
    }


  return 1;
    
}
int help(vector<string> command)
{
  if(command.size()==1)
    {
      cout<<"here is a list of all the supported commands"<<endl;
      cout<<"ls,put,get,mkdir,rm,mv,cd,exit"<<endl;
      cout<<"type: help <command> for more information"<<endl;
    }
  else
    {
      if(command.at(1)=="ls")
	cout<<"USAGE: ls <dir>"<<endl;
      if(command.at(1)=="put")
	cout<<"USAGE:put <srcfile> <dst dir>"<<endl;
      if(command.at(1)=="mkdir")
	cout<<"USAGE: mkdir <dir>"<<endl;
      if(command.at(1)=="rm")
	cout<<"USAGE:rm <file path>"<<endl;
      if(command.at(1)=="mv")
	cout<<"USAGE:mv <src> <dst>"<<endl;
      if(command.at(1)=="cd")
	cout<<"USAGE:cd <dir>"<<endl;
    }
  return 1;
}

int main(int argc, char** argv)
{
 
 
  if(argc!=3)
    {
      cout<<"USAGE:testclient <ip> <port>"<<endl;
      return -1;
    }
  Sector::init(argv[1],atoi(argv[2]));
  // user log in
  // initscr();
  string user_name;
  string pwd;
  cout<<"username:";
  cin>>user_name;
  
  cout<<"password:"<<endl;

  struct termios oldt,newt;
  tcgetattr( STDIN_FILENO, &oldt );
  newt = oldt;
  newt.c_lflag &= ~( ICANON | ECHO );
  tcsetattr( STDIN_FILENO, TCSANOW, &newt );
  cin>>pwd;
  tcsetattr( STDIN_FILENO, TCSANOW, &oldt );

 
 if (Sector::login(user_name,pwd) < 0)
   {
     cout<<"Wrong username or passwrd"<<endl;
     Sector::close();
     return -1;
   }
 else
   cout<<"login succeed!"<<endl;
 cout<<"type 'help' for help"<<endl;
 string cur_dir="/"; //pwd
 string str; 
 cout<<cur_dir<<">>";
 getline(cin,str);
 getline(cin,str);
 
 
 vector<string> command;

 parse(str,command);
 while(command.at(0)!="exit")
   {
     if (command.at(0)=="ls")
       ls(command,cur_dir);
     else if(command.at(0)=="put")
       put(command);
     else if(command.at(0)=="get")
       get(command);
     else if(command.at(0)=="mkdir")
       mkdir(command,cur_dir);
     else if(command.at(0)=="rm")
       rm(command);
     else if(command.at(0)=="mv")
       mv(command);
     else if(command.at(0)=="cd")
       cd(command,cur_dir);
     else if(command.at(0)=="help")
       help(command);
     else
       cout<<"wrong command"<<endl;

     
     cout<<cur_dir<<">>";
     getline(cin,str);
     command.clear();
     parse(str,command);
     cout<<command.at(0)<<endl;
   }
 
 
 Sector::logout();
 Sector::close();

 return 1;

}
