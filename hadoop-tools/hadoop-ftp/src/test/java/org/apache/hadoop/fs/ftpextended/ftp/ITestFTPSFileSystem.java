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
import java.net.URI;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystemTest;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test functionality of FTP file system.
 */
@RunWith(Parameterized.class)
public class ITestFTPSFileSystem extends AbstractFTPFileSystemTest {

  private static Server server;
  static final URI FTP_URI = URI.create("ftps://user:password@localhost");
  public static final String USER = "user";
  public static final String PASSWORD = "password";

  @BeforeClass
  public static void setTest() throws IOException, FtpException {
    server = new FTPSServer(TEST_ROOT_DIR);
  }

  @AfterClass
  public static void cleanTest() {
    server.stop();
  }

  @Before
  @Override
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.ftps.impl", FTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.ftps.host.port", server.getPort());
    conf.setBoolean("fs.ftps.impl.disable.cache", true);
    conf.setBoolean("fs.ftps.cache." + FTP_URI.getHost(), cache);
    setFS(FTP_URI, conf);
    super.setup();
  }
}
