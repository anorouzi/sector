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
import java.nio.channels.WritableByteChannel;

/**
 * NIO direct buffer channel for the Sector JNI interface.
 */
public class SectorOutputChannel implements WritableByteChannel
{
    private static final int BUFFER_SIZE = 1 << 20;
    private ByteBuffer writeBuffer;
    private long filedescriptor = -1;
    private SectorJniClient jniClient;

    // ** Constructors **

    public SectorOutputChannel( final long filedescriptor )
    {
        this.filedescriptor = filedescriptor;
        writeBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE );
        writeBuffer.clear();
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
     * Write data to filedescriptor.
     *
     * @param src a direct ByteBuffer
     *
     * @return number of bytes transferred.
     *
     * @throws IOException re-thrown from NIO layer if an error occurs.
     */
    public int write( ByteBuffer src )
        throws IOException
    {
        if ( filedescriptor < 0 ) {
            throw new IOException( "Cannot write to a closed file." );
        }

        long numWritten = 0L;

        final int start = src.remaining();

        while( src.hasRemaining() ) {
            int limit = src.limit();
            if ( writeBuffer.remaining() < src.remaining() ) {
                src.limit(src.position() + writeBuffer.remaining());
            }
            writeBuffer.put( src );
            writeBuffer.flip();
            numWritten = writeDirect( writeBuffer );
            writeBuffer.clear();
            src.limit( limit );
        }

        final int left = src.remaining();
        return start - left;
    }

    /**
     * Sync file.
     *
     * @throws IOException Thrown if filehandle is invalid.
     */
    public int sync()
        throws IOException
    {
        if ( filedescriptor < 0 )  {
            throw new IOException( "Cannot sync a closed file." );
        }

        writeBuffer.flip();
        writeDirect( writeBuffer );

        return 1;
    }

    /**
     * Set file offset for writing.
     *
     * @param offset New position for file offset.
     *
     * @return On sucess a non-negative value, negative value on failure.
     *
     * @throws IOException Thrown if filehandle is invalid.
    public int seek( final long offset )
        throws IOException
    {
        if ( filedescriptor < 0 ) {
            throw new IOException(
                "Cannot seek on a closed file, open it first." );
        }

        sync();
        return jniClient.sectorChannelSeekp( filedescriptor, offset );
    }

    /**
     * Return the file offset for writing.
     *
     * @return The current write pointer position.
     *
     * @throws IOException Thrown if filehandle is invalid.
     */
    public long tell()
        throws IOException
    {
        if ( filedescriptor < 0) {
            throw new IOException(
                "Cannot tell on a closed file, open it first." );
        }
        return jniClient.sectorChannelTellp( filedescriptor ) +
            writeBuffer.remaining();
    }

    /**
     * Close the channel.
     */
    public void close()
        throws IOException
    {
        if ( filedescriptor >= 0 ) {
            //sync();
            jniClient.sectorChannelClosep( filedescriptor);
            filedescriptor = -1;
        }
    }

    // ** Private Methods

    /**
     * Write data from direct buffer.
     *
     * @param buf Buffer containing data to write.
     *
     * @throws IOException Thrown if read fails.
     * @throws IllegalArgumentException Thrown if buffer is not a direct buffer.
     */
    private int writeDirect(ByteBuffer buf)
        throws IOException
    {
        if ( !buf.isDirect() ) {
            throw new IllegalArgumentException("need direct buffer");
        }
        int size = 0;
        final int position = buf.position();
        final int last = buf.limit();

        if (last - position != 0) {

            size = jniClient.sectorChannelWrite( filedescriptor, buf,
                                                 position, last );
            if (size < 0) {
                throw new IOException("writeDirect failed");
            }

            if (size == last) {
                buf.clear();
            } else if (size != 0) {
                ByteBuffer temp = ByteBuffer.allocateDirect( BUFFER_SIZE );
                temp.put( buf );
                temp.flip();
                buf.clear();
                buf.put( temp );
            }
        }

        return( size );
    }

}
