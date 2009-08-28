/*
 * Copyright (C) 2008-2009  Open Data ("Open Data" refers to
 * one or more of the following companies: Open Data Partners LLC,
 * Open Data Research LLC, or Open Data Capital LLC.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
%module sector

%{
#include "sector.h"
%}

%include "std_vector.i"
%include "std_string.i"

namespace std {
   %template(vectorsi) vector<statinfo>;
};

%{
extern int init( const char* host, int port );
extern int login( const char* user, const char* pass, const char* certpath );
extern int logout();
extern int closeclient();
extern int mkdir( const char* path );
extern int rm( const char* path );
extern int move( const char* oldpath, const char* newpath );
extern int cp( const char* path, const char* newpath );
extern long open( const char* path, int mode );
extern int close( long filehandle );
extern std::string read( long filehandle, long len );
extern int write( long filehandle, const char* data, long len );
extern int upload( const char* src, const char* dest );
extern int download( const char* src, const char* dest );
extern statinfo stat( const char* path );
extern std::vector<statinfo> ls( const char* path );
%}

%include "sector.h"
extern int init( const char* host, int port );
extern int login( const char* user, const char* pass, const char* certpath );
extern int logout();
extern int closeclient();
extern int mkdir( const char* path );
extern int rm( const char* path );
extern int move( const char* oldpath, const char* newpath );
extern int cp( const char* path, const char* newpath );
extern long open( const char* path, int mode );
extern int close( long filehandle );
extern std::string read( long filehandle, long len );
extern int write( long filehandle, const char* data, long len );
extern int upload( const char* src, const char* dest );
extern int download( const char* src, const char* dest );
extern statinfo stat( const char* path );
extern std::vector<statinfo> ls( const char* path );
