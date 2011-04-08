/**
 * Copyright (C) 2008-2011 Open Data ("Open Data" refers to
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

import java.util.List;

/**
 * JNI data class for sector system information.
 */
public class SysStat {
    
    /** The system start time */
    private long startTime;
    
    /** Total available disk space */
    private long totalDiskSpace;
    
    /** Total file size */
    private long totalFileSize;
    
    /** Total number of files */
    private long totalFileNum;
    
    /** Total number of slaves */
    private long totalSlavesNum;
    
    /** Under replicated */
    private long underReplicated;
    
    /**
     * Get the underReplicated.
     * @return long
     */
    public long getUnderReplicated() {
        return underReplicated;
    }

    /**
     * Set the underReplicated.
     * @param newUnderReplicated long
     */
    public void setUnderReplicated(long newUnderReplicated) {
        underReplicated = newUnderReplicated;
    }

    /**
     * Get the startTime.
     * @return long
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Set the startTime.
     * @param newStartTime long
     */
    public void setStartTime(long newStartTime) {
        startTime = newStartTime;
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
     * Get the totalFileNum.
     * @return long
     */
    public long getTotalFileNum() {
        return totalFileNum;
    }

    /**
     * Set the totalFileNum.
     * @param newTotalFileNum long
     */
    public void setTotalFileNum(long newTotalFileNum) {
        totalFileNum = newTotalFileNum;
    }

    /**
     * Get the totalSlavesNum.
     * @return long
     */
    public long getTotalSlavesNum() {
        return totalSlavesNum;
    }

    /**
     * Set the totalSlavesNum.
     * @param newTotalSlavesNum long
     */
    public void setTotalSlavesNum(long newTotalSlavesNum) {
        totalSlavesNum = newTotalSlavesNum;
    }

    /**
     * Get the masterList.
     * @return MasterStat[]
     */
    public MasterStat[] getMasterList() {
        return masterList;
    }

    /**
     * Set the masterList.
     * @param newMasterList MasterStat[]
     */
    public void setMasterList(MasterStat[] newMasterList) {
        masterList = newMasterList;
    }

    /**
     * Get the slaveList.
     * @return SlaveStat[]
     */
    public SlaveStat[] getSlaveList() {
        return slaveList;
    }

    /**
     * Set the slaveList.
     * @param newSlaveList SlaveStat[]
     */
    public void setSlaveList(SlaveStat[] newSlaveList) {
        slaveList = newSlaveList;
    }

    /**
     * Get the clusterList.
     * @return ClusterStat[]
     */
    public ClusterStat[] getClusterList() {
        return clusterList;
    }

    /**
     * Set the clusterList.
     * @param newClusterList ClusterStat[]
     */
    public void setClusterList(ClusterStat[] newClusterList) {
        clusterList = newClusterList;
    }

    /** Array of masters */
    private MasterStat[] masterList;
    
    /** Array of slaves */
    private SlaveStat[] slaveList;
    
    /** Array of clusters */
    private ClusterStat[] clusterList;
    
}
