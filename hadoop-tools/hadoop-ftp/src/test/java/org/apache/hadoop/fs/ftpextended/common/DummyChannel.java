/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.ftpextended.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Dummy test channel.
 */
public class DummyChannel extends AbstractChannel {

  private boolean connected = true;

  DummyChannel(ConnectionInfo info) {
    super(info);
  }

  @Override
  public boolean isAvailable() {
    return connected;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public void destroy() throws IOException {
    connected = false;
  }

  @Override
  public boolean mkdir(String parentDir, String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean rename(String oldName, String newName) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean rm(String file) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean rmdir(String dir) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String pwd() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public FileStatus[] listFiles(Path dir) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Create dummy file status. To distinguish directories file names containing
   * dot are considered files All other are considered directories.
   *
   * @param file
   * @param dirContentList
   * @return
   * @throws IOException
   */
  @Override
  public FileStatus getFileStatus(Path file, Set<FileStatus> dirContentList)
          throws IOException {
    FileStatus status = new FileStatus(0, file.isRoot() ||
            !file.getName().contains("."), 1, 0, 0, file);
    dirContentList.add(status);
    return status;
  }

  @Override
  public FSDataOutputStream put(Path file, DirTree dirTree,
          FileSystem.Statistics statistics) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public FSDataInputStream get(FileStatus file,
          FileSystem.Statistics statistics) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public InputStream getDataStream(FileStatus file) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object getNative() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
