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
import com.opendatagroup.sector.sectorjni.SysStat;
import com.opendatagroup.sector.sectorjni.MasterStat;
import com.opendatagroup.sector.sectorjni.SlaveStat;
import com.opendatagroup.sector.sectorjni.ClusterStat;

/**
 * Perform a Sector "sysinfo".
 */
public class SysInfo
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

            SysStat sysStat = client.sectorSysInfo();
            
            System.out.println("Start time: " + sysStat.getStartTime());
            System.out.println("Total available disk space: " + sysStat.getTotalDiskSpace());
            System.out.println("Total file number: " + sysStat.getTotalFileNum());
            System.out.println("Total file size: " + sysStat.getTotalFileSize());
            System.out.println("Total slave number: " + sysStat.getTotalSlavesNum());
            System.out.println("Under Replicated: " + sysStat.getUnderReplicated());
            
            //output for master stat
            MasterStat [] masterStats = sysStat.getMasterList();
            System.out.println("-----------Master Stat------------");
            for(MasterStat master : masterStats) {
                System.out.println("Master ID: " + master.getId());
                System.out.println("Master IP: " + master.getIp());
                System.out.println("Master Port: " + master.getPort());
            }
            System.out.println("-----------End of Master Stat------------");
            
            //output for cluster stat
            ClusterStat [] clusterStats = sysStat.getClusterList();
            System.out.println("-----------Cluster Stat------------");
            for(ClusterStat cluster : clusterStats) {
                System.out.println("Cluster ID: " + cluster.getId());
                System.out.println("Total Nodes Number: " + cluster.getTotalNodes());
                System.out.println("Total available disk Space: " + cluster.getTotalDiskSpace());
                System.out.println("Total file size: " + cluster.getTotalFileSize());
                System.out.println("Total input data: " + cluster.getTotalInputData());
                System.out.println("Total output data: " + cluster.getTotalOutputData());
            }
            System.out.println("-----------End of Cluster Stat------------");
            
            //output for slave stat
            SlaveStat [] slaveStats = sysStat.getSlaveList();
            System.out.println("-----------Slave Stat------------");
            for(SlaveStat slave : slaveStats) {
                System.out.println("Slave ID: " + slave.getId());
                System.out.println("Slave IP: " + slave.getIp());
                System.out.println("Slave port: " + slave.getPort());
                System.out.println("Slave cluster ID: " + slave.getClusterId());
                System.out.println("CPU used: " + slave.getCurrCpuUsed());
                System.out.println("Memory used: " + slave.getCurrMemUsed());
                System.out.println("Data dir: " + slave.getDataDir());
                System.out.println("Last update timestamp: " + slave.getLastUpdateTimestamp());
                System.out.println("Status: " + slave.getStatus());
                System.out.println("Total available disk Space: " + slave.getTotalDiskSpace());
                System.out.println("Total file size: " + slave.getTotalFileSize());
                System.out.println("Total input data: " + slave.getTotalInputData());
                System.out.println("Total output data: " + slave.getTotalOutputData());
            }
            System.out.println("-----------End of Slave Stat------------");
            
        } finally {
            client.sectorLogout();
            client.sectorCloseFS();
        }
    }
}
