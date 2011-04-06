/**
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
package com.opendatagroup.sector.sectorjni;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is the Java part of a Java Native Interface to the Sector filesystem.
 */
public class SectorJniClient
{
    private final static native long init( String host, int port );
    private final static native int login( String user,
                                           String password,
                                           String certPath,
                                           long clientptr );
    private final static native void logout( long clientptr );
    private final static native int closeFS( long clientptr );
    private final static native int mkdir( String path, long clientPtr );
    private final static native int remove( String path, long clientPtr );
    private final static native int move( String oldPath,
                                          String NewPath,
                                          long clientPtr );
    private final static native SNode stat( String path, long clientPtr );
    private final static native SNode[] list( String path, long clientPtr );
    private final static native long openFile( String filename, int mode, long clientPtr );
    private final static native int closeFile( long filehandle );
    private final static native byte[] read( long filehandle, long len );
    private final static native byte[] read( long filehandle,
                                             long offset,
                                             long len );
    private final static native int write( byte[] buf, long filehandle,
                                           long offset, long len );
    private final static native int seekg( long filehandle,
                                           long offset,
                                           int pos );
    private final static native int seekp( long filehandle,
                                           long offset,
                                           int pos );
    private final static native long tellg( long filehandle );    
    private final static native long tellp( long filehandle );
    private final static native int upload( String src, String dest, long clientPtr);
    private final static native int download( String src, String dest, long clientPtr );

    // These are called from Sector Inoput/Output Channel only
    // Input channel
    private final static native int channelRead( long fd, ByteBuffer buf,
                                                 int begin, int end );
    // Output channel
    private final static native int channelWrite( long fd, ByteBuffer buf,
                                                  int begin, int end );

    /**
     * C++ pointer to the Sector client.
     */
    private long clientPtr;

    /**
     * Mode to open file for reading.
     */
    public static final int READ = 1;
    /**
     * Mode to open file for writing.
     */
    public static final int WRITE = 2;
    /**
     * Mode to open file for read/write.
     */
    public static final int READ_WRITE = 3;

    /**
     * Relative file position for seek() - beginning of file.
     */
    public static final int POS_BEG = 1;
    /**
     * Relative file position for seek() - current file position.
     */
    public static final int POS_CUR = 2;
    /**
     * Relative file position for seek() - end of file.
     */
    public static final int POS_END = 3;
    
    // Load the Sector client shared object:
    static {
        try {
            System.loadLibrary( "SectorJniClient" );
        } catch( UnsatisfiedLinkError e ) {
            e.printStackTrace();
            System.err.println( "Unable to load sector native library" );
            System.exit( 1 );
        }
    }
    
    /**
     * Default constructor. Not much to see here.
     */
    public SectorJniClient()
    {}

    /**
     * Connect to Sector.
     *
     * @param host Hostname/IP for Sector master node.
     * @param port Port number for Sector master node.
     *
     * @return on success, a non-negative value. A negative value on failure.
     */
    public int sectorInit( String host, int port )
    {
        clientPtr = init( host, port );
        if( clientPtr == -1L ) {
            return( -1 );
        }

        return( 1 );
    }

    /**
     * Login to Sector.
     *
     * @param user Sector username.
     * @param passwd Password for user.
     * @param certPath Path to the Sector master cert. If null the default
     * path will be used.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorLogin( String user, String passwd, String certPath )
    {
        return( login( user, passwd, certPath, clientPtr ) );
    }

    /**
     * Logout of Sector.
     */
    public void sectorLogout()
    {
        logout( clientPtr );
    }

    /**
     * Close the connection to Sector and release resources. Should always be
     * called at the end of processing.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorCloseFS()
    {
        return( closeFS( clientPtr ) );
    }

    /**
     * Create a new directory in the Sector filesystem. You can pass a nested
     * directory to be created - for example /parentdir/subdir.
     *
     * @param path Path for new directory.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorMkdir( String path )
        throws IOException
    {
        return( mkdir( path, clientPtr ) );
    }

    /**
     * Remove a file or directory in the Sector filesystem. Note that if path
     * argument is a populated directory remove will perform a recursive remove
     * of the directory and all it's contents.
     *
     * @param path File/dir to be deleted.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorRemove( String path )
    {
        return( remove( path, clientPtr ) );
    }

    /**
     * Move a file or directory in the Sector filesystem.
     *
     * @param oldPath Path to file/dir to be moved.
     * @param newPath New path for file/dir.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorMove( String oldPath, String newPath )
    {
        return( move( oldPath, newPath, clientPtr ) );
    }

    /**
     * Get info on a file or directory, including size, timestamp, etc.
     *
     * @param path path for file/dir to return info on.
     *
     * @return Class encapsulating info on file/dir.
     *
     * @throws FileNotFoundException Thrown if path in non-existent.
     * @throws IOException Thrown if stat fails.
     */
    public SNode sectorStat( String path )
        throws FileNotFoundException, IOException
    {
        SNode snode = null;
        
        snode = stat( path, clientPtr );
        if( snode == null ) {
            throw new IOException( "Error stat'ing " + path );
        } 

        return( snode );
    }

    /**
     * Return info on a directory/file. Sector equivalent of the 'ls' command.
     *
     * @param path Path to return listing for.
     *
     * @return Array of classes encapsulating info on contents of path.
     *         returns a zero length array if error occurs.
     */
    public SNode[] sectorList( String path )
    {
        SNode[] snodes = list( path, clientPtr );
        if( snodes == null ) {
            return( new SNode[0] );
        }
        
        return( snodes );
    }

    /**
     * Open a Sector file. Creates a new file if it doesn't exist and mode is
     * WRITE.
     *
     * @param filename Path to file for opening.
     * @param mode Either READ, WRITE, or READ_WRITE.
     *
     * @return A long which represents a pointer to a Sector filehandle. This
     * value must be passed to subsequent operations on this file.
     *
     * @throws IOException Thrown if open fails.
     */
    public long sectorOpenFile( String filename, int mode )
        throws IOException
    {
        long fileHandle = openFile( filename, mode, clientPtr );
        if( fileHandle == -1 ) {
            throw new IOException( "Error opening file " + filename +
                                   ", mode=" + mode );
        }

        return( fileHandle );
    }

    /**
     * Close Sector file.
     *
     * @param filehandle C++ pointer returned by sectorOpenFile() call.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorCloseFile( long filehandle )
    {
        return( closeFile( filehandle ) );
    }

    /**
     * Read data from a file in Sector. This method can be called repeatedly
     * to read through a file in chunks. To do a random-access read use the
     * method that takes an offset.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     * @param len Length of data to read.
     *
     * @return On success, the data read from the file. null on failure or EOF.
     */
    public byte[] sectorRead( long filehandle, long len )
    {
        return( read( filehandle, len ) );
    }
    
    /**
     * Read data from a file in Sector.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     * @param offset File offset to start reading from.
     * @param len Length of data to read.
     *
     * @return On success, the data read from the file. null on failure or EOF.
     */
    public byte[] sectorRead( long filehandle, long offset, long len )
    {
        return( read( filehandle, offset, len ) );
    }

    /**
     * Write data to a file in Sector.
     * 
     * @param buf Data to write.
     * @param filehandle Pointer returned by sectorOpenFile() call.
     * @param offset File offset to start reading from.
     * @param len Length of data to read.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorWrite( byte[] buf, long filehandle,
                            long offset, long len )
    {
        return( write( buf, filehandle, offset, len ) );
    }

    /**
     * Set the read offset position.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     * @param offset New postion for read pointer.
     * @param pos Relative position in file to set offset from. Either
     * POS_BEG, POS_CUR, or POS_END.
     *
     * @return On sucess a non-negative value, negative value on failure.
     */
    public int sectorSeekg( long filehandle, long offset, int pos )
    {
        return( seekg( filehandle, offset, pos ) );
    }

    /**
     * Set the write offset position.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     * @param offset New postion for write pointer.
     * @param pos Relative position in file to set offset from. Either
     * POS_BEG, POS_CUR, or POS_END.
     *
     * @return On sucess a non-negative value, negative value on failure.
     */
    public int sectorSeekp( long filehandle, long offset, int pos )
    {
        return( seekp( filehandle, offset, pos ) );
    }

    /**
     * Retrieve the current read offset position.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     *
     * @return The current read pointer position.
     */
    public long sectorTellg( long filehandle )
    {
        return( tellg( filehandle ) );
    }

    /**
     * Retrieve the current write offset position.
     *
     * @param filehandle Pointer returned by sectorOpenFile() call.
     *
     * @return The current write pointer position.
     */
    public long sectorTellp( long filehandle )
    {
        return( tellp( filehandle ) );
    }
    
    /**
     * Copy a file from the local filesystem into Sector.
     *
     * @param src Path to local file to copy.
     * @param dest Path to Sector file to copy to.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorUpload( String src, String dest )
    {
        return( upload( src, dest, clientPtr ) );
    }

    /**
     * Copy a file from Sector to the local filesystem.
     *
     * @param src Path to Sector file to copy.
     * @param dest Path to local file to copy to.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorDownload( String src, String dest )
    {
        return( download( src, dest, clientPtr ) );
    }

    // Channel Methods

    /**
     * Read from a direct buffer.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     * @param buf Buffer to hold returned data.
     * @param begin Beginning position in read buffer.
     * @param end Ending position in read buffer.
     *
     * @return On success, number of bytes read. A negative value on failure.
     */
    public int sectorChannelRead( long fd, ByteBuffer buf, int begin, int end )
    {
        return ( channelRead( fd, buf, begin, end ) );
    }

    /**
     * Close the input channel.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorChannelCloseg( long fd )
    {
        return ( sectorCloseFile( fd ) );
    }

    /**
     * Set the read channel file offset position.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     * @param offset New postion for read pointer.
     *
     * @return On sucess a non-negative value, negative value on failure.
     */
    public int sectorChannelSeekg( long fd, long offset )
    {
        return ( sectorSeekg( fd, offset, POS_CUR ) );
    }

    /**
     * Retrieve the current file offset for the read channel.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     *
     * @return The current read pointer position.
     */
    public long sectorChannelTellg( long fd )
    {
        return ( sectorTellg( fd ) );
    }

    /**
     * Close the output channel.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     *
     * @return On success, a non-negative value. A negative value on failure.
     */
    public int sectorChannelClosep( long fd )
    {
        return ( sectorCloseFile( fd ) );
    }

    /**
     * Write a direct buffer.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     * @param buf Buffer containing data to write.
     * @param begin Beginning position in write buffer.
     * @param end Ending position in write buffer.
     *
     * @return On success, the number of bytes written. A negative value on
     * failure.
     */
    public int sectorChannelWrite( long fd, ByteBuffer buf, int begin, int end )
    {
        return ( channelWrite( fd, buf, begin, end ) );
    }

    /**
     * Set the write channel offset position.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     * @param offset new postion for read pointer.
     *
     * @return On sucess a non-negative value. A negative value on failure.
     */
    public int sectorChannelSeekp( long fd, long offset )
    {
        return ( sectorSeekp( fd, offset, POS_CUR ) );
    }

    /**
     * Retrieve the current file offset for the write channel.
     *
     * @param fd Sector filehandle pointer returned by sectorOpenFile() call.
     *
     * @return The current write pointer position.
     */
    public long sectorChannelTellp( long fd )
    {
        return ( sectorTellg( fd ) );
    }
}
