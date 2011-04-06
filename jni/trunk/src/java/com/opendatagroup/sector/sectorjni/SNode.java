/**
 * Copyright (C) 2008-2009 Open Data ("Open Data" refers to
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
 * Java class representing the Sector SNode structure. The SNode structure
 * contains data returned by the stat call, and encapsulates info for a
 * file or directory.
 */
public class SNode
{
    /**
     * File/directory name
     */
    private String name;
    /**
     * True if a directory.
     */
    private boolean isDir;
    /**
     * File/dir creation time.
     */
    private long timestamp;
    /**
     * File size
     */
    private long size;
    /**
     * IP address(es) of nodes where file is located.
     */
    private String[] locations;
    
    /**
     * Get the value for the file/dir name.
     *
     * @return The name value.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the value for the file/dir name.
     *
     * @param newName The new name value.
     */
    public void setName(String newName) {
        this.name = newName;
    }

    /**
     * Is this a directory?
     *
     * @return True if dir, false if file.
     */
    public boolean isDir() {
        return isDir;
    }

    /**
     * Set the IsDir value.
     *
     * @param newIsDir The new IsDir value.
     */
    public void setIsDir(boolean newIsDir) {
        this.isDir = newIsDir;
    }

    /**
     * Get the timestamp of file/dir.
     *
     * @return The Timestamp value.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set the timestamp value.
     *
     * @param newTimestamp The new Timestamp value.
     */
    public void setTimestamp(long newTimestamp) {
        this.timestamp = newTimestamp;
    }

    /**
     * Get the value for the size of file/dir.
     *
     * @return The Size value.
     */
    public long getSize() {
        return size;
    }

    /**
     * Set the size value.
     *
     * @param newSize The new Size value.
     */
    public void setSize(long newSize) {
        this.size = newSize;
    }

    /**
     * Set the list of locations for file.
     */
    public void setLocations(String[] newLocations)
    {
        locations = newLocations;
    }

    /**
     * Get the value of locations for file.
     */
    public String[] getLocations()
    {
        return locations;
    }
}
