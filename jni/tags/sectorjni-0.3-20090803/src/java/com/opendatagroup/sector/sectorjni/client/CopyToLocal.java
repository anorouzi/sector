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
package com.opendatagroup.sector.sectorjni.client;

import java.io.FileOutputStream;
import java.io.OutputStream;

import java.io.IOException;

import com.opendatagroup.sector.sectorjni.SectorJniClient;

/**
 * Copy a file from Sector to the local file system.
 */
public class CopyToLocal
{
    private static String host = System.getProperty( "sector.host" );
    private static int port = Integer.valueOf( System.getProperty( "sector.port" ) );
    private static String user = System.getProperty( "sector.user" );
    private static String pass = System.getProperty( "sector.passwd" );
    private static String certPath = System.getProperty( "sector.certpath" );
    private static int BUF_SIZE = 4096;
    
    public static void main( String[] args ) 
    {
        SectorJniClient client = new SectorJniClient();
        int status = 0;
        OutputStream out = null;
        long filehandle = 0L;
        
        if( args.length < 2 ) {
            System.out.println( "Usage: CopyToLocal src dest" );
            return;
        }

        final String srcPath = args[0];
        final String destPath = args[1];
        
        try {
            status = client.sectorInit( host, port );
            if( status < 0 ) {
                System.out.println( "Failed to initialize Sector, status=" +
                                    status );
                return;
            }
            
            status = client.sectorLogin( user, pass, certPath );
            if( status < 0 ) {
                System.out.println( "Failed to log into Sector, status=" +
                                    status );
                return;
            }

            filehandle = client.sectorOpenFile( srcPath,
                                                SectorJniClient.READ );
            out = new FileOutputStream( destPath, true );
            byte[] buffer = new byte[BUF_SIZE];
            int offset = 0;
            buffer = client.sectorRead( filehandle, offset, BUF_SIZE );
            while( buffer != null ) {
                out.write( buffer );
                offset += BUF_SIZE;
                buffer = client.sectorRead( filehandle, offset, BUF_SIZE );
            }
        } catch( IOException e ) {
            e.printStackTrace();
        } finally {
            client.sectorCloseFile( filehandle );
            try {
                out.close();
            } catch( IOException e ) {
            }
            client.sectorLogout();
            client.sectorCloseFS();
        }
    }
}
