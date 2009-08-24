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
#include <Python.h>
#include <structmember.h>
#include <iostream>
#include <string>
#include <vector>
#include "constant.h"
#include "index.h"
#include "fsclient.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;

/*
  A Python module providing a bridge between Python apps and the Sector C++
  client.
*/

/*
  A StatInfo object encapsulates the file attributes returned from the stat()
  call.
*/
typedef struct 
{
    PyObject_HEAD
    PyObject* name;
    int isDir;
    long timestamp;
    long size;
} StatInfo;

static PyMethodDef StatInfo_methods[] = {
    { NULL, NULL }
};

static PyMemberDef StatInfo_members[] = {
    { "name", T_OBJECT, offsetof( StatInfo, name ), 0, "file name" },
    { "isDir", T_INT, offsetof( StatInfo, isDir ), RO, "is this a directory?" },
    { "timestamp", T_LONG, offsetof( StatInfo, timestamp ), RO, "file timestamp" },
    { "size" , T_LONG, offsetof( StatInfo, size ), RO, "file size" },
    { NULL }
};

static void StatInfo_dealloc( PyObject* self )
{
    StatInfo* s = (StatInfo*)self;
    Py_DECREF( s->name );
    s->ob_type->tp_free( (PyObject*)s );
}

static PyObject* StatInfo_repr( PyObject* self )
{
    StatInfo *s = (StatInfo*)self;
    return( PyString_FromFormat( "StatInfo( %s, %d, %ld, %ld )",
                                 PyString_AsString( s->name ),
                                 s->isDir,
                                 s->timestamp,
                                 s->size ) );
}

static int StatInfo_print( PyObject* self, FILE* fp, int flags )
{
    StatInfo *s = (StatInfo*)self;
    fprintf( fp,
             "StatInfo( %s, %d, %ld, %ld )",
             PyString_AsString( s->name ),
             s->isDir,
             s->timestamp,
             s->size );
    return( 0 );
}

static int StatInfo_init( PyObject* self, PyObject* args, PyObject* kwds )
{
    return( 0 );
}

static PyObject* StatInfo_new( PyTypeObject* type,
                               PyObject* args,
                               PyObject* kwds )
{
    StatInfo* s = (StatInfo*)type->tp_alloc( type, 0 );
    return( (PyObject*)s );
}

/*
  Type object for StatInfo.
*/
static PyTypeObject StatInfoType = {
	PyObject_HEAD_INIT( NULL )
	0,
	"sector.StatInfo",
	sizeof( StatInfo ),
	0,
	StatInfo_dealloc,                       // tp_dealloc
	StatInfo_print,                         // tp_print 
	0,					// tp_getattr
	0,					// tp_setattr
	0,                                      // tp_compare 
	StatInfo_repr,                          // tp_repr 
	0,					// tp_as_number
	0,					// tp_as_sequence 
	0,					// tp_as_mapping 
	0,					// tp_hash
	0,					// tp_call
	0,					// tp_str 
	PyObject_GenericGetAttr,                // tp_getattro
	PyObject_GenericSetAttr,	        // tp_setattro
	0,					// tp_as_buffer
	Py_TPFLAGS_DEFAULT,                     // tp_flags 
	0,					// tp_doc
        0,		            		// tp_traverse
	0,				        // tp_clear
	0,	  	  	  	        // tp_richcompare
	0,				        // tp_weaklistoffest
	0,				        // tp_iter
	0,				        // tp_iternext
	StatInfo_methods,			// tp_methods
	StatInfo_members,		        // tp_members
	0,              		        // tp_getset
	0,				        // tp_base
	0,				        // tp_dict
	0,				        // tp_descr_get
	0,				        // tp_descr_set
	0,				        // tp_dictoffset
	StatInfo_init,                          // tp_init
	PyType_GenericAlloc,                    // tp_alloc
	StatInfo_new                            // tp_new
};

/*
 * Initialize connection to Sector.
 *
 * host is the hostname of the node running the Sector master.
 * port is the port number for the Sector master.
 *
 * Returns: A non-negative value on success, a negative value on failure.
 */
static PyObject* init( PyObject* self, PyObject* args )
{
    const char* host;
    int port;
    
    if( !PyArg_ParseTuple( args, "si", &host, &port ) ) {
        return( NULL );
    }

    int status = Sector::init( host, port );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to connect to sector, status=" +
                         status );
        return( NULL );
    }
    
    return( Py_BuildValue( "i", status ) );
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
static PyObject* login( PyObject* self, PyObject* args )
{
    const char* user;
    const char* pass;
    const char* certpath;
    
    if( !PyArg_ParseTuple( args, "sss", &user, &pass, &certpath ) ) {
        return( NULL );
    }

    int status = Sector::login( user, pass, certpath );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to log into to sector, status=" +
                         status );
        return( NULL );
    }
    
    return( Py_BuildValue( "i", status ) );
}

/*
 * Logout of Sector.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* logout( PyObject* self, PyObject* args )
{
    int status = Sector::logout();

    return( Py_BuildValue( "i", status ) );
}

/*
 * Disconnect from Sector and release resources. This should always be called
 * when processing is complete.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* closeclient( PyObject* self, PyObject* args )
{
    int status = Sector::close();

    return( Py_BuildValue( "i", status ) );
}

/*
 * Make a new directory in the Sector filesystem. This will create parent/child
 * directories if the path argument is a nested directory.
 *
 * path is full path to the new directory.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* mkdir( PyObject* self, PyObject* args )
{
    const char* path;

    if( !PyArg_ParseTuple( args, "s", &path ) ) {
        return( NULL );
    }

    int status = Sector::mkdir( path );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to create dir, status=" +
                         status );
        return( NULL );
    }
    
    return( Py_BuildValue( "i", status ) );
}

/*
 * Remove a file/dir from the Sector filesystem. This call will recursively
 * remove populated directories.
 *
 * path is full path to file/dir to be removed.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* remove( PyObject* self, PyObject* args )
{
    const char* path;

    if( !PyArg_ParseTuple( args, "s", &path ) ) {
        return( NULL );
    }

    int status = Sector::remove( path );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to remove path, status=" +
                         status );
        return( NULL );
    }
    
    return( Py_BuildValue( "i", status ) );
}

/*
 * Move "oldpath" to "newpath".
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* move( PyObject* self, PyObject* args )
{
    const char* oldpath;
    const char* newpath;
    
    if( !PyArg_ParseTuple( args, "ss", &oldpath, &newpath ) ) {
        return( NULL );
    }

    int status = Sector::move( oldpath, newpath );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "move failed, status=" + status );
        return( NULL );
    }
    
    return( Py_BuildValue( "i", status ) );
}

/*
 * Return a collection of objects containing info on a path in the Sector
 * filesystem. This is basically the Sector equivalent of "ls".
 *
 * This function is not fully implemented yet.
 *
 * path is the full path of the directory to list.
 *
 * Returns: On success, a collection of objects encapsulating info on each
 * file in the path. On failure, None.
 */
// static PyObject* list( PyObject* self, PyObject* args )
// {
//     int status = 0;
//     const char* path;
//     vector<SNode> filelist;
    
//     if( !PyArg_ParseTuple( args, "s", &path ) ) {
//         return( NULL );
//     }
    
//     status = Sector::list( path, filelist );
//     if( status < 0 ) {
//         PyErr_SetString( PyExc_IOError, "list failed, status=" + status );
//         return( NULL );
//     }
    
//     // TODO: create return object
//     return( Py_None );
// }

/*
 * Return an object containing info on a path in the Sector filesystem.
 *
 * path is the full path of the file/dir to get info on.
 *
 * Returns: on success an object encapsulating the info for the path. On
 * failure None.
 */
static PyObject* stat( PyObject* self, PyObject* args )
{
    const char* path;
    SNode attr;
    
    if( !PyArg_ParseTuple( args, "s", &path ) ) {
        return( NULL );
    }
    
    int status = Sector::stat( path, attr );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "stat failed, status=" + status );
        return( NULL );
    }

    StatInfo* s =
        (StatInfo*)StatInfoType.tp_new( &StatInfoType, NULL, NULL );
    if( s < 0 ) {
        return( NULL );
    }

//    Py_DECREF( s->name );
    s->name = PyString_FromString( attr.m_strName.c_str() );
    s->isDir = attr.m_bIsDir;
    s->timestamp = attr.m_llTimeStamp;
    s->size = attr.m_llSize;
    
    return( (PyObject*)s );
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
static PyObject* open( PyObject* self, PyObject* args )
{
    const char* path;
    int16_t mode;
    SectorFile* f = new SectorFile();
    
    if( !PyArg_ParseTuple( args, "si", &path, &mode ) ) {
        return( NULL );
    }

    int status = f->open( path, mode );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "open failed, status=" + status );
        return( NULL );
    }
    
    return( Py_BuildValue( "l", f ) );
}

/*
 * Close a file in Sector.
 *
 * filehandle is the value returned by open().
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* close( PyObject* self, PyObject* args )
{
    SectorFile *f;
    long filehandle;
    
    if( !PyArg_ParseTuple( args, "l", &filehandle ) ) {
        return( NULL );
    }

    f = (SectorFile*)filehandle;
    int status = f->close();

    return( Py_BuildValue( "i", status ) );
}

/*
 * Read data from a Sector file. This function can be called repeatedly
 * to read through a file in chunks.
 *
 * filehandle is the value returned by open().
 * len is the number of bytes to read from the file.
 *
 * Returns: on success the data read. None on EOF. On failure NULL.
 */
static PyObject* read( PyObject* self, PyObject* args )
{
    SectorFile* f;
    long filehandle;
    long len;
    
    if( !PyArg_ParseTuple( args, "ll", &filehandle, &len ) ) {
        return( NULL );
    }

    f = (SectorFile*)filehandle;

    char buf[len];
    int status = f->read( buf, len );
    if( status == 0 ) {
        return( Py_None );
    }
    
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "read failed, status=" + status );
        return( NULL );
    }

    buf[status] = '\0';
    
    return( Py_BuildValue( "s", buf ) );
}

/*
 * Write data to a Sector file.
 */
static PyObject* write( PyObject* self, PyObject* args )
{
    SectorFile* f;
    long filehandle;
    const char* data;
    long len;
    
    if( !PyArg_ParseTuple( args, "lsl", &filehandle, &data, &len ) ) {
        return( NULL );
    }

    f = (SectorFile*)filehandle;
    
    int status = f->write( data, len );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "write failed, status=" + status );
        return( NULL );
    }

    return( Py_BuildValue( "i", status ) );
}

/*
 * Copy a file from the local filesystem to Sector.
 *
 * src is the path to the local file.
 * dest is the path to the new Sector file.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
static PyObject* upload( PyObject* self, PyObject* args )
{
    SectorFile f;

    const char* src;
    const char* dest;
    
    if( !PyArg_ParseTuple( args, "ss", &src, &dest ) ) {
        return( NULL );
    }

    int status = f.open( dest, SF_MODE::WRITE );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to open destination, status=" +
                         status );
        return( NULL );
    }

    status = f.upload( src, false );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "upload failed, status=" + status );
    }
    
    f.close();

    return( Py_BuildValue( "i", status ) );
}

/*
 * Copy a file from Sector to the local filesystem.
 *
 * src is path to Sector file.
 * dest is path to new local file.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
static PyObject* download( PyObject* self, PyObject* args )
{
    int status = 0;
    SectorFile f;

    const char* src;
    const char* dest;
    
    if( !PyArg_ParseTuple( args, "ss", &src, &dest ) ) {
        return( NULL );
    }

    status = f.open( src, SF_MODE::READ );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "failed to open source, status=" +
                         status );
        return( NULL );
    }

    status = f.download( dest );
    if( status < 0 ) {
        PyErr_SetString( PyExc_IOError, "download failed, status=" + status );
    }
    
    f.close();

    return( Py_BuildValue( "i", status ) );
}

static PyMethodDef SectorMethods[] = {
    { "init", init, METH_VARARGS, "init Sector client" },
    { "login", login, METH_VARARGS, "log into Sector" },
    { "logout", logout, METH_VARARGS, "log out of Sector" },
    { "closeclient", closeclient, METH_VARARGS, "close Sector" },
    { "mkdir", mkdir, METH_VARARGS, "create a new directory in Sector" },
    { "remove", remove, METH_VARARGS, "remove a file/dir in Sector" },
    { "move", move, METH_VARARGS, "move a file/dir in Sector" },
//    { "list", list, METH_VARARGS, "perform a Sector ls" },
    { "stat", stat, METH_VARARGS, "return info on a file/dir in Sector" },
    { "open", open, METH_VARARGS, "open a Sector file" },
    { "close", close, METH_VARARGS, "close a Sector file" },
    { "read", read, METH_VARARGS, "read data from a Sector file" },
    { "write", write, METH_VARARGS, "write data to a Sector file" },
    { "upload", upload, METH_VARARGS, "upload a local file to Sector" },
    { "download", download, METH_VARARGS, "download a file from Sector" },
    { NULL, NULL, 0, NULL },
};

PyMODINIT_FUNC initsector( void )
{
    (void)Py_InitModule( "sector", SectorMethods );
}

int main( int argc, char**argv )
{
    /* Pass argv[0] to the Python interpreter */
    Py_SetProgramName(argv[0]);
    
    /* Initialize the Python interpreter.  Required. */
    Py_Initialize();

    Py_Exit( 0 );
}
