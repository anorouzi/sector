/**
 * Copyright (C) 2008-2011  Open Data ("Open Data" refers to
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

/**
 * JNI representation of slave stat struct.
 */
public class SlaveStat {

    /** Slave node ID */
    private int id;
    
    /** IP address of slave node */
    private String ip;
    
    /** Slave node port */
    private int port;

    /** Last update time */
    private long lastUpdateTimestamp;
    
    /** Total available disk space on this node */
    private long totalDiskSpace;
    
    /** Total file size on this node */
    private long totalFileSize;
    
    /** Physical memory used by this slave */
    private long currMemUsed;
    
    /** CPU time used by this slave (aggregate since start) */
    private long currCpuUsed;
    
    /** Total network input data size */
    private long totalInputData;
    
    /** Total network output data size */
    private long totalOutputData;
    
    /** Data dir */
    private String dataDir;
    
    /** Cluster ID */
    private int clusterId;
    
    /** slave node status */
    private int status;
    
    /**
     * Get the dataDir.
     * @return String
     */
    public String getDataDir() {
        return dataDir;
    }

    /**
     * Set the dataDir.
     * @param newDataDir String
     */
    public void setDataDir(String newDataDir) {
        dataDir = newDataDir;
    }

    /**
     * Get the clusterId.
     * @return int
     */
    public int getClusterId() {
        return clusterId;
    }

    /**
     * Set the clusterId.
     * @param newClusterId int
     */
    public void setClusterId(int newClusterId) {
        clusterId = newClusterId;
    }

    /**
     * Get the status.
     * @return int
     */
    public int getStatus() {
        return status;
    }

    /**
     * Set the status.
     * @param newStatus int
     */
    public void setStatus(int newStatus) {
        status = newStatus;
    }

    /**
     * Get the id.
     * @return int
     */
    public int getId() {
        return id;
    }

    /**
     * Set the id.
     * @param newId int
     */
    public void setId(int newId) {
        id = newId;
    }

    /**
     * Get the port.
     * @return int
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port.
     * @param newPort int
     */
    public void setPort(int newPort) {
        port = newPort;
    }
    
    /**
     * Get the ip.
     * @return String
     */
    public String getIp() {
        return ip;
    }

    /**
     * Set the ip.
     * @param newIp String
     */
    public void setIp(String newIp) {
        ip = newIp;
    }

    /**
     * Get the lastUpdateTimestamp.
     * @return long
     */
    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    /**
     * Set the lastUpdateTimestamp.
     * @param newLastUpdateTimestamp long
     */
    public void setLastUpdateTimestamp(long newLastUpdateTimestamp) {
        lastUpdateTimestamp = newLastUpdateTimestamp;
    }

    /**
     * Get the totalDiskSpace.
     * @return long
     */
    public long getTotalDiskSpace() {
        return totalDiskSpace;
    }

    /**
     * Set the totalDiskSpace.
     * @param newTotalDiskSpace long
     */
    public void setTotalDiskSpace(long newTotalDiskSpace) {
        totalDiskSpace = newTotalDiskSpace;
    }

    /**
     * Get the totalFileSize.
     * @return long
     */
    public long getTotalFileSize() {
        return totalFileSize;
    }

    /**
     * Set the totalFileSize.
     * @param newTotalFileSize long
     */
    public void setTotalFileSize(long newTotalFileSize) {
        totalFileSize = newTotalFileSize;
    }

    /**
     * Get the currMemUsed.
     * @return long
     */
    public long getCurrMemUsed() {
        return currMemUsed;
    }

    /**
     * Set the currMemUsed.
     * @param newCurrMemUsed long
     */
    public void setCurrMemUsed(long newCurrMemUsed) {
        currMemUsed = newCurrMemUsed;
    }

    /**
     * Get the currCpuUsed.
     * @return long
     */
    public long getCurrCpuUsed() {
        return currCpuUsed;
    }

    /**
     * Set the currCpuUsed.
     * @param newCurrCpuUsed long
     */
    public void setCurrCpuUsed(long newCurrCpuUsed) {
        currCpuUsed = newCurrCpuUsed;
    }

    /**
     * Get the totalInputData.
     * @return long
     */
    public long getTotalInputData() {
        return totalInputData;
    }

    /**
     * Set the totalInputData.
     * @param newTotalInputData long
     */
    public void setTotalInputData(long newTotalInputData) {
        totalInputData = newTotalInputData;
    }

    /**
     * Get the totalOutputData.
     * @return long
     */
    public long getTotalOutputData() {
        return totalOutputData;
    }

    /**
     * Set the totalOutputData.
     * @param newTotalOutputData long
     */
    public void setTotalOutputData(long newTotalOutputData) {
        totalOutputData = newTotalOutputData;
    }

    
}
