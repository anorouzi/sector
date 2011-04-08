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
 * JNI representation of cluster stat struct.
 */
public class ClusterStat {

    /** Cluster ID */
    private int id;
    
    /** total number of nodes in this cluster */
    private int totalNodes;
    
    /** total available disk space */
    private long totalDiskSpace;
    
    /** total file size on this node */
    private long totalFileSize;
    
    /** total network input data size */
    private long totalInputData;
    
    /** total network output data size */
    private long totalOutputData;

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
     * Get the totalNodes.
     * @return int
     */
    public int getTotalNodes() {
        return totalNodes;
    }

    /**
     * Set the totalNodes.
     * @param newTotalNodes int
     */
    public void setTotalNodes(int newTotalNodes) {
        totalNodes = newTotalNodes;
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
