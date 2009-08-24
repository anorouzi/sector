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

import java.io.IOException;

import com.opendatagroup.sector.sectorjni.SectorJniClient;

/**
 * Remove a file/dir from the Sector file system.
 */
public class Remove
{
    private static String host = System.getProperty( "sector.host" );
    private static int port = Integer.valueOf( System.getProperty( "sector.port" ) );
    private static String user = System.getProperty( "sector.user" );
    private static String pass = System.getProperty( "sector.passwd" );
    private static String certPath = System.getProperty( "sector.certpath" );

    public static void main( String[] args )
    {
        SectorJniClient client = new SectorJniClient();
        int status = 0;

        if( args.length < 1 ) {
            System.out.println( "You must pass in the path to be removed" );
            return;
        }

        final String path = args[0];

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

            status = client.sectorRemove( path );
            if( status < 0 ) {
                System.out.println( "Failed to remove " + path + ", status=" +
                                    status );
                return;
            }
        } finally {
            client.sectorLogout();
            client.sectorCloseFS();
        }
    }
}
