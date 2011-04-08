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
#include <jni.h>
#include <string>
#include "com_opendatagroup_sector_sectorjni_SectorJniClient.h"
#include "sector.h"
#include "sphere.h"
#include "osportable.h"

using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::iterator;
using std::set;
using std::string;
using std::vector;

/*
 * C++ code for implementing a Java Native Interface to the Sector filesystem.
 */

inline string getStr( JNIEnv*, jstring );
jobject createSNode( JNIEnv*, jclass, SNode  );
jobject createMasterStat(JNIEnv* env, const SysStat::MasterStat& masterStat);
jobject createSlaveStat(JNIEnv* env, const SysStat::SlaveStat& slaveStat);
jobject createClusterStat(JNIEnv* env, const SysStat::ClusterStat& clusterStat);
jobject createSysStat(JNIEnv* env, const SysStat& sysStat);

/*
 * JNI uses field descriptors to represent Java field types. These are used
 * when we're retrieving fields from Java objects:
 */
const char* STRING_TYPE = "Ljava/lang/String;";
const char* BOOLEAN_TYPE = "Z";
const char* LONG_TYPE = "J";
const char* TIMESTAMP_TYPE = "J";
const char* INT_TYPE = "I";
const char* VOID_TYPE = "()V";

/*
 * Initialize connection to Sector. This function returns a long which
 * represents a pointer to a Sector client object. This pointer is passed
 * to subsequent operations requiring a reference to the client.
 *
 * host is the hostname of the node running the Sector master.
 * port is the port number for the Sector master.
 *
 * Returns: on success, a pointer to a Sector client encoded as a long. On
 * failure, -1.
 */
JNIEXPORT jlong JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_init( JNIEnv* env,
                                                              jclass jcls,
                                                              jstring host,
                                                              jint port )
{
    Sector* client = new Sector();
    string hoststr = getStr( env, host );
    if( hoststr.empty() ) {
        cerr << "SectorJniClient.init(): no value received for Sector host" <<
            endl;
        return( (jlong)-1 );
    }
    
    int status = client->init( hoststr.c_str(), port );
    if( status < 0 ) {
        // Apparently in some cases the value returned by converting the client
        // pointer to a long can be negative. If there is an error just return
        // -1 to make it explicit:
        return( (jlong)-1 );
    }
    
    return( (jlong)client );
}

/*
 * Login to Sector.
 *
 * user/passwd are login credentials for a registered Sector user.
 * certPath is a path to the Sector master cert file. If null then the
 * default path will be used.
 *
 * cptr is the pointer to the Sector client returned by the init call.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_login( JNIEnv* env,
                                                               jclass jcls,
                                                               jstring user,
                                                               jstring passwd,
                                                               jstring certPath,
                                                               jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;

    string userstr = getStr( env, user );
    if( userstr.empty() ) {
        cerr << "SectorJniClient.login(): user value is empty" << endl;
        return( -1 );
    }

    string passwdstr = getStr( env, passwd );
    if( passwdstr.empty() ) {
        cerr << "SectorJniClient.login(): passwd value is empty" << endl;
        return( -1 );
    }

    string pathstr;
    if( certPath != NULL ) {
        pathstr = getStr( env, certPath );
    } else {
        pathstr = "";
    }

    return( clientPtr->login( userstr.c_str(),
                              passwdstr.c_str(),
                              ( !pathstr.empty() ? pathstr.c_str() : NULL ) ) );
}

/*
 * Logout of Sector.
 *
 * cptr is the pointer to the Sector client returned by the init() call.
 */
JNIEXPORT void JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_logout( JNIEnv* env,
                                                                jclass jcls,
                                                                jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;
    clientPtr->logout();
}

/*
 * Disconnect from Sector and release resources. This should always be called
 * when processing is complete.
 *
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_closeFS( JNIEnv* env,
                                                                 jclass jcls,
                                                                 jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;
    int status = clientPtr->close();
    free( clientPtr );
    return( status );
}

/*
 * Make a new directory in the Sector filesystem. This will create parent/child
 * directories if the path argument is a nested directory.
 *
 * path is full path to the new directory.
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_mkdir( JNIEnv* env,
                                                               jclass jcls,
                                                               jstring path,
                                                               jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;

    string pathstr = getStr( env, path );
    if( pathstr.empty() ) {
        cerr << "SectorJniClient.mkdir(): path value is empty" << endl;
        return( -1 );
    }

    return( clientPtr->mkdir( pathstr ) );
}

/*
 * Remove a file/dir from the Sector filesystem. This call will recursively
 * remove populated directories.
 *
 * path is full path to file/dir to be removed.
 *
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_remove( JNIEnv* env,
                                                                jclass jcls,
                                                                jstring path,
                                                                jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;

    string pathstr = getStr( env, path );
    if( pathstr.empty() ) {
        cerr << "SectorJniClient.remove(): path value is empty" << endl;
        return( -1 );
    }

    return( clientPtr->remove( pathstr ) );
}

/*
 * Move a file from "oldPath" to "newPath".
 *
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: on success a non-negative value. A negative value on failure.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_move( JNIEnv* env,
                                                              jclass jcls,
                                                              jstring oldPath,
                                                              jstring newPath,
                                                              jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;

    string oldstr = getStr( env, oldPath );
    if( oldstr.empty() ) {
        cerr << "SectorJniClient.move(): value for old path is empty" << endl;
        return( -1 );
    }

    string newstr = getStr( env, newPath );
    if( newstr.empty() ) {
        cerr << "SectorJniClient.move(): value for new path is empty" << endl;
        return( -1 );
    }

    int status = clientPtr->move( oldstr, newstr );
    if( status < 0 ) {
        cerr << "SectorJniClient.move(): error moving " << oldstr <<
            " to " << newstr << ", status=" << status << endl;
    }
    
    return( status );
}

/*
 * Return a com.opendatagoup.sector.sectorjni.SNode class containing info
 * on a file/dir in Sector.
 *
 * path is the full path of the file/dir to get info on.
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: on success the SNode object encapsulating the info on path. On
 * failure NULL.
 *
 * Throws FileNotFoundException if path isn't found.
 */
JNIEXPORT jobject JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_stat( JNIEnv* env,
                                                              jclass jcls,
                                                              jstring path,
                                                              jlong cptr )
{
    Sector* clientPtr = (Sector*)cptr;
    SNode attr;

    string pathstr = getStr( env, path );
    if( pathstr.empty() ) {
        cerr << "SectorJniClient.stat(): path value is empty" << endl;
        return( NULL );
    }

    int status = clientPtr->stat( pathstr, attr );
    if( status < 0 ) {
        cerr << "SectorJniClient.stat(): Unable to locate file: " << pathstr
             << ", status=" << status << endl;
        if( SectorError::E_NOEXIST == status ) {
            jclass ex = env->FindClass( "java/io/FileNotFoundException" );
            env->ThrowNew( ex, ( "Unable to find " + pathstr ).c_str() );
        }
    }

    // Get a reference to the Java SNode class:
    jclass cls = env->FindClass( "com/opendatagroup/sector/sectorjni/SNode" );
    if( cls == NULL ) {
        cerr << "SectorJniClient.stat(): Could not find SNode class" << endl;
        return( NULL );
    }

    // Create a new SNode instance:
    jobject obj = createSNode( env, cls, attr );
    if( obj == NULL ) {
        return( NULL );
    }

    return( obj );
}

/*
 * Return an array of com.opendatagoup.sector.sectorjni.SNode classes
 * containing info on the contents of a directory. This is basically the
 * Sector equivalent of "ls".
 *
 * path is the full path of the directory to list.
 *
 * cptr is the pointer to the Sector client returned by the init() call.
 *
 * Returns: On success, an array of SNode objects. On failure, NULL.
 */
JNIEXPORT jobjectArray JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_list( JNIEnv* env,
                                                              jclass jcls,
                                                              jstring path,
                                                              jlong cptr)
{
    Sector* clientPtr = (Sector*)cptr;
    jobjectArray snodes;
    
    string pathstr = getStr( env, path );
    if( pathstr.empty() ) {
        cerr << "SectorJniClient.list(): path value is empty" << endl;
        return( NULL );
    }

    // Get the file listing from Sector:
    vector<SNode> filelist;
    if( clientPtr->list( pathstr, filelist ) < 0 ) {
        cerr << "Unable to get listing for: " << path << endl;
        return( NULL );
    }

    // Get a reference to the Java SNode class:
    jclass cls = env->FindClass( "com/opendatagroup/sector/sectorjni/SNode" );
    if( cls == NULL ) {
        cerr << "SectorJniClient.list(): Could not find SNode class" << endl;
        return( NULL );
    }

    // Initialize the return array:
    snodes = env->NewObjectArray( filelist.size(), cls, NULL );

    // Populate the return array:
    for( vector<SNode>::size_type i = 0; i < filelist.size(); i++ ) {
        SNode snode = filelist[i];
        jobject obj = createSNode( env, cls, snode );
        if( obj == NULL ) {
            return( NULL );
        }

        env->SetObjectArrayElement( snodes, i, obj );
    }

    return( snodes );
}

/*
 * Open file "filename" in Sector. Returns a long representing a pointer to
 * the Sector filehandle. This pointer needs to be passed to subsequent
 * file operations.
 *
 * mode is READ ONLY (1), WRITE ONLY (2), or READ/WRITE (3).
 *
 * Return: on success, a pointer to the Sector filehandle. On failure, -1.
 */
JNIEXPORT jlong JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_openFile( JNIEnv* env,
                                                                  jclass jcls,
                                                                  jstring filename,
                                                                  jint mode,
                                                                  jlong cptr )
{
    Sector* sector = (Sector*)cptr;
    SectorFile* f = sector->createSectorFile();

    string filestr = getStr( env, filename );
    if( filestr.empty() ) {
        cerr << "SectorJniClient.openFile(): filename value is empty" << endl;
        return( (jlong)-1 );
    }

    long status = f->open( filestr.c_str(), mode  );
    // Apparently in some cases the value returned by converting the pointer
    // to a long can be negative. If there is an error just return -1 to 
    // make it explicit:
    if( status < 0 ) {
        cerr << "SectorJniClient.openFile() - error opening file: " <<
            filestr << ", status=" << status << endl;
        return( (jlong)-1 );
    }

    return( (jlong)f );
}

/*
 * Close Sector file.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_closeFile( JNIEnv* env,
                                                                   jclass jcls,
                                                                   jlong filehandle )
{
    SectorFile* f = (SectorFile*)filehandle;

    // TODO add a check to make sure file isn't already closed and free'd.
    int status = f->close();
    if( status < 0 ) {
        cerr << "SectorJniClient.closeFile(): error closing file, " <<
            "filehandle=" << filehandle << ", status=" << status << endl;
    }
    
    free( f );
    return( status );
}

/*
 * Read data from a Sector file. This function can be called repeatedly
 * to read through a file in chunks. To do a random-access read use the
 * function that takes an offset.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * len is amount of data to read.
 *
 * Returns: on success the data read. On failure or EOF, NULL.
 */
JNIEXPORT jbyteArray JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_read__JJ( JNIEnv* env,
                                                                  jclass jcls,
                                                                  jlong filehandle,
                                                                  jlong len )
{
    return( Java_com_opendatagroup_sector_sectorjni_SectorJniClient_read__JJJ(
                env, jcls, filehandle, -1, len ) );
}
 
/*
 * Read data from a Sector file.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * offset is file offset to start reading from. Pass in -1 to use the seek
 * pointer maintained by SectorFile.
 * len is amount of data to read.
 *
 * Returns: on success the data read. On failure or EOF, NULL.
 */
JNIEXPORT jbyteArray JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_read__JJJ( JNIEnv* env,
                                                                   jclass jcls,
                                                                   jlong filehandle,
                                                                   jlong offset,
                                                                   jlong len )
{
    SectorFile* f = (SectorFile*)filehandle;
    int status;
    jbyteArray ret = NULL;

    // TODO: Do we need the EOF check?
    if( !f->eof() ) {
        
        if( offset >= 0 ) {
            status = f->seekg( offset, SF_POS::BEG );
            if( status < 0 ) {
                cerr << "SectorJniClient.read(): seekg() to offset " <<
                    offset << " failed" << endl;
                return( NULL );
            }
        }
        
        char* buf = new char[len];
        
        status = f->read( buf, len );

        if( status < 0 ) {
            cerr << "SectorJniClient.read(): read failed, status=" << status <<
                endl;
            ret = NULL;
        } else {
            // Create and populate the return byte array:
            ret = env->NewByteArray( status );
            env->SetByteArrayRegion( ret, 0, status, (jbyte*)buf );
        }
    
        free( buf );
    } // end if( !f->eof()

    return( ret );
}

/*
 * Write data to a file.
 *
 * data is data to write to file.
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * offset is file offset to start writing to.
 * len is amount of data to write.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_write( JNIEnv* env,
                                                               jclass jcls,
                                                               jbyteArray data,
                                                               jlong filehandle,
                                                               jlong offset,
                                                               jlong len )
{
    int status = 0;
    SectorFile* f = (SectorFile*)filehandle;

    // Set file position pointer to offset value:
    status = f->seekp( offset, SF_POS::CUR );
    if( status < 0 ) {
        cerr << "SectorJniClient.write(): seekp() to offset " << offset <<
            " failed" << endl;
        return( -1 );
    }

    // Get the byte array containing the data to write...
    jbyte* jbyteptr = env->GetByteArrayElements( data, 0 );
    // ...and convert it to a char array required by the Sector call:
    char* cptr = (char *)jbyteptr;

    status = f->write( cptr, len );
    if( status < 0 ) {
        cerr << "SectorJniClient.write(): write failed, return code is " <<
            status << ", filehandle=" << filehandle << endl;
    }
    
    env->ReleaseByteArrayElements( data, jbyteptr, 0 );
    
    return( status );
}

/**
 * Set the read offset position.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * offset is the new postion for read pointer.
 * pos is the relative position in file to set offset from. This is either
 * SF_POS::BEG (1), SF_POS::CUR (2), or SF_POS::END (3)
 *
 * Returns: on succss, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
 Java_com_opendatagroup_sector_sectorjni_SectorJniClient_seekg( JNIEnv* env,
                                                                jclass jcls,
                                                                jlong filehandle,
                                                                jlong offset,
                                                                jint pos )
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->seekg( offset, pos ) );
}

/*
 * Set the write offset position.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * offset is the new postion for read pointer.
 * pos is the relative position in file to set offset from. This is either
 * SF_POS::BEG (1), SF_POS::CUR (2), or SF_POS::END (3)
 *
 * Returns: on succss, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_seekp( JNIEnv* env,
                                                               jclass jcls,
                                                               jlong filehandle,
                                                               jlong offset,
                                                               jint pos )
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->seekp( offset, pos ) );
}

/*
 * Retrieve the current read offset positon.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 *
 * Returns: the current read pointer position.
 */
JNIEXPORT jlong JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_tellg( JNIEnv* env,
                                                               jclass jcls,
                                                               jlong filehandle )
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->tellg() );
}

/*
 * Retrieve the current write offset positon.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 *
 * Returns: the current write pointer position.
 */
JNIEXPORT jlong JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_tellp( JNIEnv* env,
                                                               jclass jcls,
                                                               jlong filehandle)
{
    SectorFile* f = (SectorFile*)filehandle;
    return( f->tellp() );
}

/*
 * Copy file from local filesystem into Sector.
 *
 * src is path to local file.
 * dest is path to new Sector file.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_upload( JNIEnv* env,
                                                                jclass jcls,
                                                                jstring src,
                                                                jstring dest,
                                                                jlong cptr)
{
    Sector* sector = (Sector*)cptr;
    SectorFile* f = sector->createSectorFile();
    int status = 0;

    string deststr = getStr( env, dest );
    if( deststr.empty() ) {
        cerr <<
            "SectorJniClient.upload(): destination filename value is empty"
             << endl;
        return( -1 );
    }

    string srcstr = getStr( env, src );
    if( srcstr.empty() ) {
        cerr <<
            "SectorJniClient.upload(): source filename value is empty" << endl;
        return( -1 );
    }
    
    status = f->open( deststr.c_str(), SF_MODE::WRITE );
    if( status < 0 ) {
        cerr << "SectorJniClient.upload(): error opening destination file " <<
            deststr << ", status=" << status << endl;
        return( status );
    }
    
    status = f->upload( srcstr.c_str(), false );
    if( status < 0 ) {
        cerr << "SectorJniClient.upload(): error uploading file " <<
            srcstr << ", status=" << status << endl;
        return( status );
    }

    f->close();

    return( 0 );
}

/*
 * Copy file from Sector to local filesystem.
 *
 * src is path to Sector file.
 * dest is path to new local file.
 *
 * Returns: on success, a non-negative value. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_download( JNIEnv* env,
                                                                  jclass cls,
                                                                  jstring src,
                                                                  jstring dest,
                                                                  jlong cptr)
{
    Sector* sector = (Sector*)cptr;
    SectorFile* f = sector->createSectorFile();
    int status = 0;
    
    string deststr = getStr( env, dest );
    if( deststr.empty() ) {
        cerr <<
            "SectorJniClient.upload(): destination filename value is empty" <<
            endl;
        return( -1 );
    }

    string srcstr = getStr( env, src );
    if( srcstr.empty() ) {
        cerr <<
            "SectorJniClient.upload(): source filename value is empty" << endl;
        return( -1 );
    }

    status = f->open( srcstr.c_str(), SF_MODE::READ );
    if( status < 0 ) {
        cerr << "SectorJniClient.download(): error opening source file " <<
            srcstr << ", status=" << status << endl;
        return( status );
    }

    status = f->download( deststr.c_str() );
    if( status < 0 ) {
        cerr << "SectorJniClient.download(): error downloading file " <<
            srcstr << ", status=" << status << endl;
        return( status );
    }

    f->close();

    return( 0 );
}

/*
 * Get Sector sys info.
 */
JNIEXPORT jobject JNICALL Java_com_opendatagroup_sector_sectorjni_SectorJniClient_sysInfo(
                                                        JNIEnv * env,
                                                        jclass cls,
                                                        jlong cptr) 
{
    Sector* sector = (Sector*)cptr;
    SysStat sysStat;

    int result = sector->sysinfo(sysStat);
    if( result < 0 ) {
        cerr << "SectorJniClient.sysInfo(): error get sys info" << "result=" << result << endl;
        return NULL;
    }

    jobject obj = createSysStat( env, sysStat);
    if( obj == NULL ) {
        return( NULL );
    }

    return( obj );
}

/*
 * Convert jstring in "src" to a C++ string. Returns an empty string if
 * memory allocation error occurs.
 */
inline string getStr( JNIEnv* env, jstring src )
{
    string s;
    const char* buf = env->GetStringUTFChars( src, 0 );
    if( buf != NULL ) {
        s = string( buf );
    }
    
    env->ReleaseStringUTFChars( src, buf );
    return( s );
}

/*
 * Create and populate a Java SNode instance.
 *
 * The following are the steps required to create an instance of a Java object
 * in JNI:
 *   1. Get a reference to the class using FindClass.
 *   2. Get a method ID for the constructor using GetMethodID().
 *   3. Use the class reference and constructor method ID to get a new
 *      object instance through the NewObject() function.
 *   4. Populate each object field:
 *     4.1. Get a field ID using getFieldID()
 *     4.2. Set the field in the object using the the field ID and the instance
 *          returned in step 3. Fields are set through Set<type>Field().
 */
jobject createSNode( JNIEnv* env, jclass snodeCls, SNode cppSNode  )
{
    // Get constructor for Java SNode:
    jmethodID constr = env->GetMethodID( snodeCls, "<init>", VOID_TYPE );
    if( constr == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Could not get constructor for SNode class"
             << endl;
        return( NULL );
    }

    // Get a new instance of the Java SNode class:
    jobject obj = env->NewObject( snodeCls, constr );
    if( obj == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Could not instantiate SNode" <<
            endl;
    }

    // Populate SNode fields:
    jfieldID fid = env->GetFieldID( snodeCls, "name", STRING_TYPE );
    if( fid == NULL ) {
        return( NULL );
    }
    jstring name = env->NewStringUTF( cppSNode.m_strName.c_str() );
    if( name == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Error getting name field" <<
            endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, name );
    
    fid = env->GetFieldID( snodeCls, "isDir", BOOLEAN_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Error getting isDir field" <<
            endl;
        return( NULL );
    }
    env->SetBooleanField( obj, fid, cppSNode.m_bIsDir );

    fid = env->GetFieldID( snodeCls, "size", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Error getting size field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, cppSNode.m_llSize );

    fid = env->GetFieldID( snodeCls, "timestamp", TIMESTAMP_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Error getting timestamp field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, cppSNode.m_llTimeStamp );

    fid = env->GetFieldID( snodeCls, "locations", "[Ljava/lang/String;" );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSNode(): Error getting locations field" <<
            endl;
        return( NULL );
    }

    jclass jstrClass = env->FindClass("Ljava/lang/String;");
    jobjectArray arr = env->NewObjectArray( cppSNode.m_sLocation.size(),
                                            jstrClass, NULL );

     Address addr;
     jstring ip;
     int i = 0;
     for( set<Address, AddrComp>::iterator iter = cppSNode.m_sLocation.begin();
          iter != cppSNode.m_sLocation.end(); iter++ ){
         addr = *iter;
         ip = env->NewStringUTF( addr.m_strIP.c_str() );
         env->SetObjectArrayElement( arr, i++, ip );
     }

     env->SetObjectField( obj, fid, arr );
    
     return( obj );
}

/*
 * Create Master Stat object 
 */
jobject createMasterStat(JNIEnv* env, const SysStat::MasterStat& masterStat) {
    
    // get MasterStat class
    jclass cls = env->FindClass("com/opendatagroup/sector/sectorjni/MasterStat");
    if(cls == NULL) {
        cerr << "Failed to create MasterStat. MasterStat not found" << endl;
        return NULL;
    }

    // get constructor
    jmethodID constr = env->GetMethodID(cls, "<init>", VOID_TYPE);
    if(constr == NULL) {
        cerr << 
            "Failed to create MasterStat, MasterStat constructor not found" << endl;
        return NULL;
    }

    // create a new object
    jobject obj = env->NewObject(cls, constr);
    if(obj == NULL) {
        cerr << 
            "Failed to create MasterStat, calling constructor failed" << endl;
        return NULL;
    }

    // Populate MasterStat fields:
    jfieldID fid = env->GetFieldID( cls, "id", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createMasterStat(): Error getting id field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, masterStat.m_iID);

    fid = env->GetFieldID(cls, "ip", STRING_TYPE);
    jstring ip = env->NewStringUTF( masterStat.m_strIP.c_str() );
    if( ip == NULL ) {
        cerr <<
            "SectorJniClient.createMasterStat(): Error getting ip field" << endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, ip);

    fid = env->GetFieldID( cls, "port", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createMasterStat(): Error getting port field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, masterStat.m_iPort);

    return (obj);
}

/*
 * Create Slave Stat object 
 */
jobject createSlaveStat(JNIEnv* env, const SysStat::SlaveStat& slaveStat) {
    
    // get SlaveStat class
    jclass cls = env->FindClass("com/opendatagroup/sector/sectorjni/SlaveStat");
    if(cls == NULL) {
        cerr << "Failed to create SlaveStat. SlaveStat not found" << endl;
        return NULL;
    }

    // get constructor
    jmethodID constr = env->GetMethodID(cls, "<init>", VOID_TYPE);
    if(constr == NULL) {
        cerr << 
            "Failed to create SlaveStat, SlaveStat constructor not found" << endl;
        return NULL;
    }

    // create a new object
    jobject obj = env->NewObject(cls, constr);
    if(obj == NULL) {
        cerr << 
            "Failed to create SlaveStat, calling constructor failed" << endl;
        return NULL;
    }

    // Populate SlaveStat fields:
    jfieldID fid = env->GetFieldID( cls, "id", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting id field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, slaveStat.m_iID);

    fid = env->GetFieldID(cls, "ip", STRING_TYPE);
    jstring ip = env->NewStringUTF( slaveStat.m_strIP.c_str() );
    if( ip == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting ip field" << endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, ip);

    fid = env->GetFieldID( cls, "port", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting port field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, slaveStat.m_iPort);
    
    fid = env->GetFieldID( cls, "lastUpdateTimestamp", TIMESTAMP_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting timestamp field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llTimeStamp );

    fid = env->GetFieldID( cls, "totalDiskSpace", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting totalDiskSpace field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llAvailDiskSpace);

    fid = env->GetFieldID( cls, "totalFileSize", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting totalFileSize field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llTotalFileSize);

    fid = env->GetFieldID( cls, "currMemUsed", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting currMemUsed field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llCurrMemUsed);

    fid = env->GetFieldID( cls, "currCpuUsed", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting currCpuUsed field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llCurrCPUUsed);

    fid = env->GetFieldID( cls, "totalInputData", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting totalInputData field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llTotalInputData);

    fid = env->GetFieldID( cls, "totalOutputData", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting totalOutputData field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, slaveStat.m_llTotalOutputData);

    fid = env->GetFieldID(cls, "dataDir", STRING_TYPE);
    jstring dataDir = env->NewStringUTF( slaveStat.m_strDataDir.c_str() );
    if( dataDir == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting dataDir field" << endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, dataDir);

    fid = env->GetFieldID( cls, "clusterId", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting clusterId field" <<
            endl;
        return( NULL );
    }
    env->SetIntField( obj, fid, slaveStat.m_iClusterID);

    fid = env->GetFieldID( cls, "status", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSlaveStat(): Error getting status field" <<
            endl;
        return( NULL );
    }
    env->SetIntField( obj, fid, slaveStat.m_iStatus);

    return (obj);
}

/*
 * Create Cluster Stat object 
 */
jobject createClusterStat(JNIEnv* env, const SysStat::ClusterStat& clusterStat) {
    
    // get ClusterStat class
    jclass cls = env->FindClass("com/opendatagroup/sector/sectorjni/ClusterStat");
    if(cls == NULL) {
        cerr << "Failed to create ClusterStat. ClusterStat not found" << endl;
        return NULL;
    }

    // get constructor
    jmethodID constr = env->GetMethodID(cls, "<init>", VOID_TYPE);
    if(constr == NULL) {
        cerr << 
            "Failed to create ClusterStat, ClusterStat constructor not found" << endl;
        return NULL;
    }

    // create a new object
    jobject obj = env->NewObject(cls, constr);
    if(obj == NULL) {
        cerr << 
            "Failed to create ClusterStat, calling constructor failed" << endl;
        return NULL;
    }

    // Populate MasterStat fields:
    jfieldID fid = env->GetFieldID( cls, "id", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting id field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, clusterStat.m_iClusterID);

    fid = env->GetFieldID( cls, "totalNodes", INT_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting totalNodes field" << endl;
        return( NULL );
    }
    env->SetIntField(obj, fid, clusterStat.m_iTotalNodes);

    fid = env->GetFieldID( cls, "totalDiskSpace", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting totalDiskSpace field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, clusterStat.m_llAvailDiskSpace);

    fid = env->GetFieldID( cls, "totalFileSize", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting totalFileSize field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, clusterStat.m_llTotalFileSize);

    fid = env->GetFieldID( cls, "totalInputData", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting totalInputData field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, clusterStat.m_llTotalInputData);

    fid = env->GetFieldID( cls, "totalOutputData", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createClusterStat(): Error getting totalOutputData field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, clusterStat.m_llTotalOutputData);

    return (obj);
}

/*
 * Create SysStat object 
 */
jobject createSysStat(JNIEnv* env, const SysStat& sysStat) {
    // master stat array    
    jobjectArray masterStats;

    // slave stat array
    jobjectArray slaveStats;

    // cluster stat array
    jobjectArray clusterStats;

    // get SysStat class
    jclass cls = env->FindClass("com/opendatagroup/sector/sectorjni/SysStat");
    if(cls == NULL) {
        cerr << "Failed to create SysStat. SysStat not found" << endl;
        return NULL;
    }

    // get constructor
    jmethodID constr = env->GetMethodID(cls, "<init>", VOID_TYPE);
    if(constr == NULL) {
        cerr << 
            "Failed to create SysStat, SysStat constructor not found" << endl;
        return NULL;
    }

    // create a new object
    jobject obj = env->NewObject(cls, constr);
    if(obj == NULL) {
        cerr << 
            "Failed to create SysStat, calling constructor failed" << endl;
        return NULL;
    }

    // Populate SysStat fields:
    jfieldID fid = env->GetFieldID( cls, "startTime", TIMESTAMP_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting id field" << endl;
        return( NULL );
    }
    env->SetLongField(obj, fid, sysStat.m_llStartTime);

    fid = env->GetFieldID( cls, "startTime", TIMESTAMP_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting id field" << endl;
        return( NULL );
    }
    env->SetLongField(obj, fid, sysStat.m_llStartTime);

    fid = env->GetFieldID( cls, "totalDiskSpace", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting totalDiskSpace field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, sysStat.m_llAvailDiskSpace);

    fid = env->GetFieldID( cls, "totalFileSize", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting totalFileSize field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, sysStat.m_llTotalFileSize);

    fid = env->GetFieldID( cls, "totalFileNum", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting totalFileNum field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, sysStat.m_llTotalFileNum);

    fid = env->GetFieldID( cls, "underReplicated", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting underReplicated field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, sysStat.m_llUnderReplicated);

    fid = env->GetFieldID( cls, "totalSlavesNum", LONG_TYPE );
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting totalSlavesNu field" <<
            endl;
        return( NULL );
    }
    env->SetLongField( obj, fid, sysStat.m_llTotalSlaves);

    //Master nodes array
    jclass masterStatClazz = env->FindClass( "com/opendatagroup/sector/sectorjni/MasterStat" );
    if( masterStatClazz == NULL ) {
        cerr << "SectorJniClient.createSysStat(): Could not find MasterStat class" << endl;
        return( NULL );
    }
    masterStats = env->NewObjectArray(sysStat.m_vMasterList.size(), masterStatClazz, NULL );
    for( vector<SysStat::MasterStat>::size_type i = 0; i < sysStat.m_vMasterList.size(); i++ ) {

        SysStat::MasterStat ms = sysStat.m_vMasterList[i];
        jobject obj = createMasterStat( env, ms);
        if( obj == NULL ) {
            cerr << "SectorJniClient.createSysStat(): Could not create MasterStat object" << endl;
            return( NULL );
        }

        env->SetObjectArrayElement( masterStats, i, obj );
    }
    fid = env->GetFieldID(cls, "masterList", "[Lcom/opendatagroup/sector/sectorjni/MasterStat;");
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting masterList field" <<
            endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, masterStats);

    //Slave nodes array
    jclass slaveStatClazz = env->FindClass( "com/opendatagroup/sector/sectorjni/SlaveStat" );
    if( slaveStatClazz == NULL ) {
        cerr << "SectorJniClient.createSysStat(): Could not find SlaveStat class" << endl;
        return( NULL );
    }
    slaveStats = env->NewObjectArray(sysStat.m_vSlaveList.size(), slaveStatClazz, NULL );
    for( vector<SysStat::SlaveStat>::size_type i = 0; i < sysStat.m_vSlaveList.size(); i++ ) {

        SysStat::SlaveStat ss = sysStat.m_vSlaveList[i];
        jobject obj = createSlaveStat(env, ss);
        if( obj == NULL ) {
            cerr << "SectorJniClient.createSysStat(): Could not create SlaveStat object" << endl;
            return( NULL );
        }

        env->SetObjectArrayElement( slaveStats, i, obj );
    }
    fid = env->GetFieldID(cls, "slaveList", "[Lcom/opendatagroup/sector/sectorjni/SlaveStat;");
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting slaveList field" <<
            endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, slaveStats);

    //Clusters array
    jclass clusterStatClazz = env->FindClass( "com/opendatagroup/sector/sectorjni/ClusterStat" );
    if( clusterStatClazz == NULL ) {
        cerr << "SectorJniClient.createSysStat(): Could not find ClusterStat class" << endl;
        return( NULL );
    }
    clusterStats = env->NewObjectArray(sysStat.m_vCluster.size(), clusterStatClazz, NULL );
    for( vector<SysStat::ClusterStat>::size_type i = 0; i < sysStat.m_vCluster.size(); i++ ) {

        SysStat::ClusterStat cs = sysStat.m_vCluster[i];
        jobject obj = createClusterStat(env, cs);
        if( obj == NULL ) {
            cerr << "SectorJniClient.createSysStat(): Could not create ClusterStat object" << endl;
            return( NULL );
        }

        env->SetObjectArrayElement( clusterStats, i, obj );
    }
    fid = env->GetFieldID(cls, "clusterList", "[Lcom/opendatagroup/sector/sectorjni/ClusterStat;");
    if( fid == NULL ) {
        cerr <<
            "SectorJniClient.createSysStat(): Error getting clusterList field" <<
            endl;
        return( NULL );
    }
    env->SetObjectField( obj, fid, clusterStats);
   
    return ( obj );
}

// Channel Methods

/*
 * Read from a file into a direct buffer.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * buf is the direct buffer to contain the data read.
 * begin is the beginning position in the read buffer.
 * end is the ending position in the read buffer.
 *
 * Returns: on success, the number of bytes read. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_channelRead(
    JNIEnv* env,
    jclass jcls,
    jlong filehandle,
    jobject buf,
    jint begin,
    jint end )
{
    int status = 0;
    SectorFile* f = ( SectorFile* )filehandle;

    if ( !buf ) {
        return 0;
    }

    void * addr = env->GetDirectBufferAddress( buf );
    jlong cap = env->GetDirectBufferCapacity( buf );

    if( !addr || cap < 0 ) {
        return 0;
    }
    if( begin < 0 || end > cap || begin > end ) {
        return 0;
    }

    addr = (void *)(uintptr_t(addr) + begin);

    if ( !f->eof() ) {
        status = f->read( (char*)addr, end - begin );
        if ( status < 0 ) {
            cerr << "SectorJniClient.read(): read failed, status=" << status <<
                endl;
            return -1;
        }
    }

    return( (jint)status );
}

/*
 * Write from a direct buffer to a file.
 *
 * filehandle is pointer to the Sector filehandle returned by open() call.
 * buf is the direct buffer containing the data to write.
 * begin is the beginning position in the read buffer.
 * end is the ending position in the read buffer.
 *
 * Returns: on success, the number of bytes read. On failure, a negative value.
 */
JNIEXPORT jint JNICALL
Java_com_opendatagroup_sector_sectorjni_SectorJniClient_channelWrite(
    JNIEnv* env,
    jclass jcls,
    jlong filehandle,
    jobject buf,
    jint begin,
    jint end )
{
    SectorFile* f = (SectorFile*)filehandle;
    if ( !buf ) {
        return 0;
    }

    void * addr = env->GetDirectBufferAddress( buf );
    jlong cap = env->GetDirectBufferCapacity( buf );

    if( !addr || cap < 0 ) {
        return 0;
    }
    if( begin < 0 || end > cap || begin > end ) {
        return 0;
    }

    addr = ( void * )( uintptr_t( addr ) + begin );

    int status = f->seekp( begin, SF_POS::CUR );
    if( status < 0 ) {
        cerr << "SectorJniClient.write(): seekp() to offset " << begin <<
            " failed" << endl;
        return( -1 );
    }

    status = f->write( (const char*)addr, end - begin );
    if( status < 0 ) {
        cerr << "SectorJniClient.write(): write failed, return code is " <<
            status << ", filehandle=" << filehandle << endl;
    }

    return( (jint)status );
}

int main()
{
    return 0;
}
