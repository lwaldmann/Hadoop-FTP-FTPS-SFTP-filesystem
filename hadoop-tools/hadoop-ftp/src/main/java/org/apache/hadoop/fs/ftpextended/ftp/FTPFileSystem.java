/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ftpextended.ftp;

import java.io.IOException;
import org.apache.hadoop.fs.ftpextended.common.AbstractChannel;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import org.apache.hadoop.fs.ftpextended.common.Channel;
import org.apache.hadoop.fs.ftpextended.common.ConnectionInfo;
import java.util.function.Function;

/**
 * FTP FileSystem.
 */
public class FTPFileSystem extends AbstractFTPFileSystem {

  private static final int DEFAULT_FTP_PORT = 21;

  @Override
  protected int getDefaultPort() {
    return DEFAULT_FTP_PORT;
  }

  @Override
  protected Channel connect() throws IOException {
    return super.connect();
  }

  @Override
  protected Function<ConnectionInfo, ? extends AbstractChannel>
        getChannelSupplier() {
    return FTPChannel::create;
  }
}
