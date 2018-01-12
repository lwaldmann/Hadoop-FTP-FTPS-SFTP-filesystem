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
package org.apache.hadoop.fs.ftpextended.ftp;

import java.io.IOException;
import java.net.URI;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import org.apache.hadoop.fs.ftpextended.common.Channel;
import org.apache.hadoop.fs.ftpextended.common.ConnectionInfo;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.apache.hadoop.fs.ftpextended.contract.FTPContractTestMixin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Tests of FTP channel creation.
 */
public class ITestFTPChannel {

  protected static final String TEST_ROOT_DIR
          = FTPContractTestMixin.getRandomizedTempPath();

  @Rule
  public Timeout testTimeout = new Timeout(1 * 60 * 1000);

  private AbstractFTPFileSystem ftpFs;
  private URI uriInfo;

  private static Server server;

  @BeforeClass
  public static void setTest() throws IOException, FtpException {
    server = new FTPServer(TEST_ROOT_DIR);
  }

  @AfterClass
  public static void cleanTest() {
    server.stop();
  }

  @Before
  public void setup() throws IOException, Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.ftp.impl", FTPFileSystem.class, FileSystem.class);
    conf.setClass("fs.ftps.impl", FTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.ftp.host.port", server.getPort());
    conf.setBoolean("fs.ftp.impl.disable.cache", true);
    conf.set("fs.ftp.proxy.host", "localhost");
    uriInfo = URI.create("ftp://user:password@localhost");
    ftpFs = (AbstractFTPFileSystem) FileSystem.get(uriInfo, conf);
  }

  @After
  public void teardown() throws IOException, InterruptedException {
    ftpFs.close();
  }

  @Test
  public void testProxyNone() throws Exception {
    Configuration conf = new Configuration(ftpFs.getConf());
    conf.setEnum("fs.ftp.proxy.type", AbstractFTPFileSystem.ProxyType.NONE);
    ConnectionInfo info = new ConnectionInfo(FTPChannel::create, uriInfo, conf,
            0);
    Channel channel = FTPChannel.create(info);
    assertNotNull(channel);
    assertTrue(
            "No proxy used therefore class client class shouldn't be proxied",
            FTPPatchedClient.class.equals(channel.getNative().getClass()));
  }

  @Test
  public void testProxyHTTP() throws Exception {
    HttpProxyServer httpServer
            = DefaultHttpProxyServer
                    .bootstrap()
                    .withPort(0)
                    .start();
    try {
      Configuration conf = new Configuration(ftpFs.getConf());
      conf.setInt("fs.ftp.proxy.port", httpServer.getListenAddress().getPort());
      conf.setEnum("fs.ftp.proxy.type", AbstractFTPFileSystem.ProxyType.HTTP);
      ConnectionInfo info
              = new ConnectionInfo(FTPChannel::create, uriInfo, conf, 0);
      Channel channel = FTPChannel.create(info);
      assertNotNull(channel);
      assertTrue(
              "Proxy used therefore class client class should be instance " +
                      "of FTPHTTPTimeoutClient",
              channel.getNative() instanceof FTPHTTPTimeoutClient);
    } finally {
      httpServer.stop();
    }
  }

  @Test
  public void testProxySocks() throws Exception {
    Configuration conf = new Configuration(ftpFs.getConf());
    conf.setEnum("fs.ftp.proxy.type", AbstractFTPFileSystem.ProxyType.SOCKS4);
    ConnectionInfo info = new ConnectionInfo(FTPChannel::create, uriInfo, conf,
            0);
    Channel channel = FTPChannel.create(info);
    assertNull(channel);

    conf.setEnum("fs.ftp.proxy.type", AbstractFTPFileSystem.ProxyType.SOCKS5);
    info = new ConnectionInfo(FTPChannel::create, uriInfo, conf, 0);
    channel = FTPChannel.create(info);
    assertNull(channel);
  }

}
