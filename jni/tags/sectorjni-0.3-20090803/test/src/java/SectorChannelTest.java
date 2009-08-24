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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import com.opendatagroup.sector.channel.SectorInputChannel;
import com.opendatagroup.sector.channel.SectorOutputChannel;
import com.opendatagroup.sector.sectorjni.SNode;
import com.opendatagroup.sector.sectorjni.SectorJniClient;

public class SectorChannelTest
{
    private String host;
    private int port;
    private String user;
    private String pass;
    private String certPath;
    private String localPath = System.getProperty( "user.dir" ) + "/build.xml";
    private String dfsPath = "/build.xml";
    
    @Before
    public void initialize()
    {
        host = System.getProperty( "sector.host" );
        port = Integer.valueOf( System.getProperty( "sector.port" ) );
        user = System.getProperty( "sector.user" );
        pass = System.getProperty( "sector.passwd" );
        certPath = System.getProperty( "sector.cert.path" );
    }

    @Test
    public void testChannelReadAndWrite()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        SectorOutputChannel sout = null;
        SectorInputChannel sin = null;
        InputStream in = null;
        OutputStream out = null;
        int bufSize = 4096;
        File f = new File( localPath );
        long localFileLen = f.length();
        String downloadPath = "/tmp/channel_read_test";
    
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            long filehandle = client.sectorOpenFile( dfsPath,
                                                     SectorJniClient.WRITE );
            sout = new SectorOutputChannel( filehandle );
            in =
                new BufferedInputStream( new FileInputStream( localPath ) );
            byte[] buffer = new byte[bufSize];
            int bytesRead;
            while( ( bytesRead = in.read( buffer ) ) >= 0 ) {
                sout.write( ByteBuffer.wrap( buffer, 0, bytesRead ) );
            }
            in.close();
            sout.close();
            SNode snode = client.sectorStat( dfsPath );
            assertTrue( "sizes of source and destination files don't match",
                        f.length() == snode.getSize() );
            filehandle = client.sectorOpenFile( dfsPath,
                                                     SectorJniClient.READ );
            sin =  new SectorInputChannel( filehandle );
            byte[] b = new byte[bufSize];
            out = new FileOutputStream( downloadPath );
            while( ( status =
                     sin.read( ByteBuffer.wrap( b, 0, bufSize ) ) ) > 0 ) {
                out.write( b, 0, status );
            }
            out.close();
            sin.close();
            File f2 = new File( downloadPath );
            assertTrue( "sizes of source and destination files don't match",
                        f.length() == f2.length() );
            f2.delete();
        } catch( IOException e ){
            fail( "Caught IOException during testChannelWrite(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( dfsPath );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }
}
