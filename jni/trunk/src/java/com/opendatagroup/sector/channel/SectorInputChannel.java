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
package com.opendatagroup.sector.channel;

import com.opendatagroup.sector.sectorjni.SectorJniClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * NIO direct buffer channel for the Sector JNI interface.
 */
public class SectorInputChannel implements ReadableByteChannel
{
    private static final int DEFAULT_BUF_SIZE = 1 << 21;
    private ByteBuffer readBuffer;

    private long filedescriptor = -1;

    private SectorJniClient jniClient;


    // ** Constructors **

    public SectorInputChannel( final long filedescriptor )
    {
        readBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        readBuffer.flip();

        this.filedescriptor = filedescriptor;

        jniClient = new SectorJniClient();
    }


    // ** Public Methods **

    /**
     * Is the channel open?
     *
     * @return true if open, false otherwise.
     */
    public boolean isOpen()
    {
        return filedescriptor > 0;
    }

    /**
     * Read data from filedescriptor.
     *
     * @param dest A direct ByteBuffer.
     *
     * @return Number of bytes transferred, or -1 on EOF.
     *
     * @throws IOException Re-thrown from NIO layer if an error occurs.
     */
    public int read( ByteBuffer dest )
        throws IOException
    {
        if ( filedescriptor < 0 ) {
            throw new IOException( "Cannot read a closed file, open it first" );
        }

        final int start = dest.remaining();
        while ( dest.hasRemaining() ) {
            if ( !readBuffer.hasRemaining() ) {
                readBuffer.clear();
                readDirect( readBuffer );
                readBuffer.flip();

                if ( !readBuffer.hasRemaining() ) {
                    break;
                }
            }

            final int limit = readBuffer.limit();
            if ( dest.remaining() < readBuffer.remaining() ) {
                readBuffer.limit( readBuffer.position() + dest.remaining() );
            }

            dest.put( readBuffer );
            readBuffer.limit( limit );
        }

        final int left = dest.remaining();
        if ( left < start || start == 0 ) {
            return start - left;
        }
        return -1;
    }

    /**
     * Set the file offset for reading.
     *
     * @param offset New position for read pointer.
     *
     * @return On sucess a non-negative value, negative value on failure.
     *
     * @throws IOException Thrown if filehandle is invalid.
     */
    public int seek( final long offset )
        throws IOException
    {
        if ( filedescriptor < 0 ) {
            throw new IOException(
                "Cannot seek on a closed file, open it first." );
        }

        readBuffer.clear();
        readBuffer.flip();
        return jniClient.sectorChannelSeekg( filedescriptor, offset );
    }

    /**
     * Return the file offset for reading.
     *
     * @return The current read pointer position.
     *
     * @throws IOException Thrown if filehandle is invalid.
     */
    public long tell()
        throws IOException
    {
        if (filedescriptor < 0) {
            throw new IOException( "Cannot tell closed file, open it first." );
        }
        return jniClient.sectorChannelTellg( filedescriptor ) -
            readBuffer.remaining();
    }

    /**
     * Close the channel.
     */
    public void close()
    {
        if ( filedescriptor >= 0 ) {
            jniClient.sectorChannelCloseg( filedescriptor );
            filedescriptor = -1;
        }
    }


    // ** Private Methods **

    /**
     * Read data into a direct buffer.
     *
     * @param buf Buffer to hold returned data.
     *
     * @throws IOException Thrown if read fails.
     * @throws IllegalArgumentException Thrown if buffer is not a direct buffer.
     */
    private void readDirect( ByteBuffer buf )
        throws IOException
    {
        if ( !buf.isDirect() ) {
            throw new IllegalArgumentException("A direct buffer must be used,");
        }

        final int pos = buf.position();
        final int size = jniClient.sectorChannelRead( filedescriptor, buf,
                                                      pos, buf.limit());
        if ( size < 0 ) {
            throw new IOException("readDirect failed");
        }
        
        buf.position( pos + size );
    }
    
}
