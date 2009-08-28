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
#include <iostream>
#include <string>
#include <vector>
#include "constant.h"
#include "index.h"
#include "fsclient.h"
#include "sector.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

/*
 * Initialize connection to Sector.
 *
 * host is the hostname of the node running the Sector master.
 * port is the port number for the Sector master.
 *
 * Returns: A non-negative value on success, a negative value on failure.
 */
int init( const char* host, int port )
{
    int status = Sector::init( host, port );
    if( status < 0 ) {
        cerr << "init() failed, return=" << status << endl;
    }
    
    return( status );
}

/*
 * Login to Sector.
 *
 * user/passwd are login credentials for a registered Sector user.
 * certPath is a path to the Sector master cert file. If null then the
 * default path will be used.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int login( const char* user, const char* pass, const char* certpath )
{
    int status = ( Sector::login( user, pass, certpath ) );
    if( status < 0 ) {
        cerr << "login() failed, return=" << status << endl;
    }

    return( status );
}

/*
 * Logout of Sector.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int logout()
{
    return( Sector::logout() );
}

/*
 * Disconnect from Sector and release resources. This should always be called
 * when processing is complete.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int closeclient()
{
    return( Sector::close() );
}

/*
 * Make a new directory in the Sector filesystem. This will create parent/child
 * directories if the path argument is a nested directory.
 *
 * path is full path to the new directory.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int mkdir( const char* path )
{
    return( Sector::mkdir( path ) );
}

/*
 * Remove a file/dir from the Sector filesystem. This call will recursively
 * remove populated directories.
 *
 * path is full path to file/dir to be removed.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int rm( const char* path )
{
    return( Sector::remove( path ) );
}

/*
 * Move "oldpath" to "newpath".
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int move( const char* oldpath, const char* newpath )
{
    return( Sector::move( oldpath, newpath ) );
}

/*
 * Copy "path" to "newpath".
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int cp( const char* path, const char* newpath )
{
    return( Sector::copy( path, newpath ) );
}

/*
 * Open a file in Sector. Returns a long representing a pointer to
 * the Sector filehandle. This pointer needs to be passed to subsequent
 * file operations.
 *
 * path is the full path of the file to be opened.
 * mode is READ ONLY (1), WRITE ONLY (2), or READ/WRITE (3).
 *
 * Returns: a long representing a pointer to the Sector filehandle.
 */
long open( const char* path, int filemode )
{
    SectorFile* f = new SectorFile();

    int status = f->open( path, filemode );
    if( status < 0 ) {
        cerr << "Error opening " << path << ", return=" << status << endl;
        return( -1 );
    }

    return( (long)f );
}

/*
 * Close a file in Sector.
 *
 * filehandle is the value returned by open().
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int close( long filehandle )
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->close() );
}

/*
 * Read data from a Sector file. This function can be called repeatedly
 * to read through a file in chunks.
 *
 * filehandle is the value returned by open().
 * len is the number of bytes to read from the file.
 *
 * Returns: on success the data read. An empty buffer on EOF. On failure NULL.
 */
string read( long filehandle, long len )
{
    SectorFile* f = (SectorFile*)filehandle;
    char* buf = new char[len];
    
    int status = f->read( buf, len );
    if( status < 0 ) {
        cerr << "Error reading data, return=" << status << endl;
        return( NULL );
    }

    buf[status] = '\0';

    string s( buf );

    free( buf );
    
    return( s );
}

/*
 * Write data to a Sector file.
 *
 * filehandle is the value returned by open().
 * data if a buffer the data to be written.
 * len is the length of the data being written.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int write( long filehandle, const char* data, long len )
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->write( data, len ) );
}

/*
 * Copy a file from the local filesystem to Sector.
 *
 * src is the path to the local file.
 * dest is the path to the new Sector file.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
int upload( const char* src, const char* dest ) 
{
    SectorFile f;
    int status = f.open( dest, SF_MODE::WRITE );
    if( status < 0 ) {
        return( status );
    }

    status = f.upload( src, false );
    f.close();

    return( status );
}

/*
 * Copy a file from Sector to the local filesystem.
 *
 * src is path to Sector file.
 * dest is path to new local file.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
int download( const char* src, const char* dest )
{
    SectorFile f;
    int status = f.open( src, SF_MODE::READ );
    if( status < 0 ) {
        return( status );
    }

    status = f.download( dest );
    f.close();

    return( status );
}

/*
 * Return an object containing info on a path in the Sector filesystem.
 *
 * path is the full path of the file/dir to get info on.
 *
 * Returns: on success an object encapsulating the info for the path. On
 * failure None.
 */
statinfo stat( const char* path )
{
    SNode attr;
    statinfo s;
    
    int status = Sector::stat( path, attr );
    if( status < 0 ) {
        cerr << "stat() failed, return=" << status << endl;
        return( s );
    }

    s.name = attr.m_strName;
    s.isdir = attr.m_bIsDir;
    s.timestamp = attr.m_llTimeStamp;
    s.size = attr.m_llSize;

    cout << "name=" << s.name << ", isdir=" << s.isdir << ", size=" << s.size << endl;
    
    return( s );
}

/*
 * Return a collection of objects containing info on a path in the Sector
 * filesystem. This is basically the Sector equivalent of "ls".
 *
 * path is the full path of the directory to list.
 *
 * Returns: On success, a collection of objects encapsulating info on each
 * file in the path. On failure, NULL.
 */
vector<statinfo> ls( const char* path )
{
    vector<SNode> filelist;
    vector<statinfo> ret;
    
    int status = Sector::list( path, filelist );
    if( status < 0 ) {
        cerr << "list failed, return=" << status << endl;
    }

    for( vector<SNode>::size_type i = 0; i < filelist.size(); i++ ) {
        SNode snode = filelist[i];
        statinfo s;
        s.name = snode.m_strName;
        s.isdir = snode.m_bIsDir;
        s.timestamp = snode.m_llTimeStamp;
        s.size = snode.m_llSize;
        ret.push_back( s );
    }

    return( ret );
}
