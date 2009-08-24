/**
 * Copyright (C) 2008  Open Data ("Open Data" refers to
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import com.opendatagroup.sector.sectorjni.SectorJniClient;
import com.opendatagroup.sector.sectorjni.SNode;

/**
 * Unit tests for JNI interface to Sector.
 */
public class SectorJniClientTest
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
    public void testInit()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
        } finally {
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void testLogin()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            client.sectorLogout();
        } finally {
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }
    
    @Test
    public void testOpenNew()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String path = "/new_file_test.dat";
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            long filehandle = client.sectorOpenFile( path,
                                                     SectorJniClient.WRITE );
            client.sectorCloseFile( filehandle );
            SNode snode = client.sectorStat( path );
            assertTrue( "Error stat'ing " + path, snode != null );
        } catch( IOException e ) {
            fail( "Caught IOException during testOpenNew(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( path );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void testReadWrite()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String path = "/read_write_test.dat";
        String data = "some test data";
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            long filehandle = client.sectorOpenFile( path,
                                                     SectorJniClient.WRITE );
            status = client.sectorWrite( data.getBytes(), filehandle,
                                         0, data.length() );
            assertTrue( ( "error writing data, status=" + status ),
                        status >= 0 );
            client.sectorCloseFile( filehandle );
            filehandle = client.sectorOpenFile( path, SectorJniClient.READ );
            byte[] b = client.sectorRead( filehandle, 0, data.length() );
            assertTrue( "return from read is null", b != null );
            String s = new String( b );
            assertTrue( ("data read doesn't match data written" +
                         "data read=" + s ),
                        s.compareTo( data ) == 0 );
            client.sectorCloseFile( filehandle );
        } catch( IOException e ) {
            fail( "Caught IOException during testReadWrite(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( path );
            try {
                client.sectorStat( path );
                fail( "Expected FileNotFoundException from stat'ing " +
                      "non-existent path" );
            } catch( FileNotFoundException e ){
            } catch( IOException e ) {
                fail( "Caught IOException: " + e );
            }
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void testAppend()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String path = "/append_test.dat";
        String data = "some test data";
        String moredata = " some appended data";

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            long filehandle = client.sectorOpenFile( path,
                                                     SectorJniClient.WRITE );
            status = client.sectorWrite( data.getBytes(), filehandle,
                                         0, data.length() );
            assertTrue( ( "error writing data, status=" + status ),
                        status >= 0 );
            client.sectorCloseFile( filehandle );
            SNode snode = client.sectorStat( path );
            if( snode == null ) {
                fail( "stat'ing " + path + " failed" );
            }
            long filesize = snode.getSize();
            filehandle = client.sectorOpenFile( path,
                                                SectorJniClient.WRITE );
            status = client.sectorWrite( moredata.getBytes(), filehandle,
                                         filesize, moredata.length() );
            assertTrue( ( "error appending data, status=" + status ),
                        status >= 0 );
            client.sectorCloseFile( filehandle );
            filehandle = client.sectorOpenFile( path, SectorJniClient.READ );
            String s =
                new String( client.sectorRead( filehandle, 0,
                                               ( data.length() +
                                                 moredata.length() ) ) );
            assertTrue( "return from read is null", s != null );
            assertTrue( ( "data read doesn't match data written, data read=" + s ),
                        s.compareTo( data + moredata ) == 0 );
            client.sectorCloseFile( filehandle );
        } catch( IOException e ) {
            fail( "Caught IOException during testReadWrite(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( path );
            try {
                client.sectorStat( path );
                fail( "Expected FileNotFoundException from stat'ing " +
                      "non-existent path" );
            } catch( FileNotFoundException e ){
            } catch( IOException e ) {
                fail( "Caught IOException: " + e );
            }
            client.sectorLogout();
            client.sectorCloseFS();
        }
    }
                        
    @Test
    public void testMkdir()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String parentPath = "/parentdir";
        String subPath = "/subdir";
        String fullPath = parentPath + subPath;
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorMkdir( fullPath );
            assertTrue( ( "error creating " + fullPath +
                          ", status=" + status ), status >= 0 );
            SNode snode = client.sectorStat( fullPath );
            assertTrue( "error stat'ing path " + fullPath,
                        snode != null );
            assertTrue( "isDir should be true for " + fullPath,
                        snode.isDir() == true );
            status = client.sectorRemove( parentPath );
            assertTrue( ( "error removing " + fullPath +
                          "status=" + status ), status >= 0 );
        } catch( IOException e ) {
            fail( "Caught IOException during testMkdir(): " +
                  e.getMessage() );
        } finally {
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void testStat()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String parentPath = "/parentdir";
        String subPath = "/subdir";
        String fullPath = parentPath + subPath;
        String testfile = "stat_test.dat";
        String testfilePath = fullPath + "/" + testfile;
        String data = "some test data";

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorMkdir( fullPath );
            assertTrue( ( "error creating new dir status=" + status ),
                        status >= 0 );
            long filehandle = client.sectorOpenFile( testfilePath,
                                                     SectorJniClient.WRITE );
            status = client.sectorWrite( data.getBytes(), filehandle, 0,
                                         data.length() );
            assertTrue( ( "testStat(): error writing data, " +
                          "status=" + status ), status >= 0 );
            client.sectorCloseFile( filehandle );

            SNode snode = client.sectorStat( parentPath );
            if( snode == null ) {
                fail( "stat'ing " + parentPath + " failed" );
            }
            assertTrue( ( "isDir should be true for " + parentPath ),
                        snode.isDir() == true );

            snode = client.sectorStat( testfilePath );
            if( snode == null ) {
                fail( "stat'ing " + testfilePath + " failed" );
            }
            assertTrue( "file name doesn't match",
                        snode.getName().compareTo( testfile ) == 0 );
            assertTrue( "file size is wrong",
                        snode.getSize() == data.length() );
            String[] locations = snode.getLocations();
            assertTrue( "error getting file locations",
                        locations.length >= 1 );
        } catch( IOException e ) {
            fail( "Caught IOException during testStat(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( parentPath );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void TestStatNonExistentFile()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            try {
                client.sectorStat( "/Idontexist" );
                fail( "Expected FileNotFoundException from stat'ing " +
                      "non-existent path" );
            } catch( FileNotFoundException e ){
            }
        } catch( IOException e ) {
            fail( "Caught IOException: " + e );
        } finally {
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }

    @Test
    public void testStatHome()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            SNode snode = client.sectorStat( "/" );
            if( snode == null ) {
                fail( "stat'ing / failed" );
            }
            assertTrue( ( "isDir should be true for /" ),
                        snode.isDir() == true );
        } catch( IOException e ) {
            fail( "Caught IOException during testStat(): " +
                  e.getMessage() );
        } finally {
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }
    
    @Test
    public void testList()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String srcPath = localPath;
        String dir = "/listtest_dir";
        String destPath = dir + dfsPath;
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorUpload( srcPath, destPath );
            assertTrue( ( "error uploading " + srcPath +
                          " to Sector, " + "status=" + status ),
                        status >= 0 );
            SNode[] snodes = client.sectorList( dir );
            assertTrue( "wrong number of files returned", snodes.length == 1 );
            File f = new File( localPath );
            assertTrue( "file size is incorrect",
                        f.length() == snodes[0].getSize() );
        } finally {
            client.sectorRemove( dir );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "testList(): error closing Sector, " +
                          "status=" + status ), status >= 0 );
        }
    }

    @Test
    public void testUploadAndDownload()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String srcPath = localPath;
        String destPath = dfsPath;
        String downloadPath = "/tmp/download_test";
        
        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorUpload( srcPath, destPath );
            assertTrue( ( "error uploading " + srcPath +
                          " to Sector, " + "status=" + status ),
                        status >= 0 );
            SNode snode = client.sectorStat( destPath );
            assertTrue( "error stat'ing uploaded file " + destPath,
                        snode != null );
            status = client.sectorDownload( dfsPath, downloadPath );
            assertTrue( ( "error downloading file, status=" + status ),
                        status >= 0 );
            File f = new File( downloadPath );
            assertTrue( "sizes of uploaded and downloaded files don't match",
                        f.length() == snode.getSize() );
        } catch( IOException e ) {
            fail( "Caught IOException during testUpload(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( dfsPath );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
            File f = new File( downloadPath );
            f.delete();
        }
    }

    @Test
    public void testSeekAndTell()
    {
        int status = 0;
        SectorJniClient client = new SectorJniClient();
        String srcPath = localPath;
        String destPath = dfsPath;

        try {
            status = client.sectorInit( host, port );
            assertTrue( ( "error connecting to Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorLogin( user, pass, certPath );
            assertTrue( ( "error logging into Sector, status=" + status ),
                        status >= 0 );
            status = client.sectorUpload( srcPath, destPath );
            assertTrue( ( "error uploading " + srcPath +
                          " to Sector, " + "status=" + status ),
                        status >= 0 );
            long filehandle =
                client.sectorOpenFile( destPath,
                                       SectorJniClient.READ_WRITE );
            status = client.sectorSeekg( filehandle, 100L,
                                         SectorJniClient.POS_BEG );
            assertTrue( "failed to set read pointer", status > 0 );
            long l = client.sectorTellg( filehandle );
            assertTrue( "error getting file position", l == 100L );
            client.sectorCloseFile( filehandle );
        } catch( IOException e ) {
            fail( "Caught IOException during testSeekTell(): " +
                  e.getMessage() );
        } finally {
            client.sectorRemove( destPath );
            client.sectorLogout();
            status = client.sectorCloseFS();
            assertTrue( ( "error closing Sector, status=" + status ),
                        status >= 0 );
        }
    }
}
