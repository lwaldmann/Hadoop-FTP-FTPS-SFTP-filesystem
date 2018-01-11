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
package org.apache.hadoop.fs.ftpextended.sftp;

import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import net.sourceforge.jsocks.ProxyServer;
import net.sourceforge.jsocks.server.ServerAuthenticatorNone;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import org.apache.hadoop.fs.ftpextended.common.ConnectionInfo;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Tests of SFTP channel creation.
 */
public class ITestSFTPChannel {
  private static Server server;
  protected static final String TEST_ROOT_DIR
          = GenericTestUtils.getRandomizedTempPath();

  @Rule
  public Timeout testTimeout = new Timeout(1 * 60 * 1000);

  private static final Logger LOG = LoggerFactory.getLogger(
          ITestSFTPChannel.class);
  private AbstractFTPFileSystem ftpFs;
  private URI uriInfo;

  private static class SocksProxyServer extends ProxyServer {

    volatile private boolean canConnect = false;

    SocksProxyServer() {
      super(new ServerAuthenticatorNone());
    }

    int getPort() throws IllegalAccessException {
      ServerSocket s = (ServerSocket) FieldUtils.readField(this, "ss", true);
      return s.getLocalPort();
    }

    @Override
    public void start(int port, int backlog, InetAddress localIP) {
      canConnect = true;
      super.start(port, backlog, localIP);
    }

    public boolean canContinue() {
      return canConnect;
    }
  }

  @BeforeClass
  public static void setTest() throws IOException, FtpException {
    server = new SFTPServer(TEST_ROOT_DIR);
  }

  @AfterClass
  public static void cleanTest() {
    server.stop();
  }

  @Before
  public void setup() throws IOException, Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.sftp.host.port", server.getPort());
    conf.setBoolean("fs.sftp.impl.disable.cache", true);
    conf.set("fs.sftp.proxy.host", "localhost");
    uriInfo = URI.create("sftp://user:password@localhost");
    ftpFs = (AbstractFTPFileSystem) FileSystem.get(uriInfo, conf);
  }

  @After
  public void teardown() throws IOException, InterruptedException {
    ftpFs.close();
  }

  @Test
  public void testProxyNone() throws Exception {
    LOG.info("testProxyNone");
    Configuration conf = new Configuration(ftpFs.getConf());
    conf.setEnum("fs.sftp.proxy.type", AbstractFTPFileSystem.ProxyType.NONE);
    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info);
    assertNotNull(channel);
    Session s = channel.getNative().getSession();
    Object o = FieldUtils.readField(s, "proxy", true);
    assertNull(o);
  }

  @Test
  public void testProxySocks4() throws Exception {
    LOG.info("testProxySocks4");
    SocksProxyServer proxy = new SocksProxyServer();
    new Thread(() -> {
      proxy.start(0);
    }).start();
    try {
      while (!proxy.canContinue()) {
        Thread.sleep(100);
      }
      Configuration conf = new Configuration(ftpFs.getConf());
      LOG.info("Connecting to the port: " + proxy.getPort());
      conf.setInt("fs.sftp.proxy.port", proxy.getPort());
      conf.setEnum("fs.sftp.proxy.type",
              AbstractFTPFileSystem.ProxyType.SOCKS4);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info);
      assertNotNull(channel);
      Session s = channel.getNative().getSession();
      Object o = FieldUtils.readField(s, "proxy", true);
      assertNotNull(o);
      assertTrue("Type of proxy should be SOCKS4", o instanceof ProxySOCKS4);
    } finally {
      proxy.stop();
    }
  }

  @Test
  public void testProxySocks5() throws Exception {
    LOG.info("testProxySocks5");
    SocksProxyServer proxy = new SocksProxyServer();
    new Thread(() -> {
      proxy.start(0);
    }).start();
    try {
      while (!proxy.canContinue()) {
        Thread.sleep(100);
      }
      Configuration conf = new Configuration(ftpFs.getConf());
      LOG.info("Connecting to the port: " + proxy.getPort());
      conf.setInt("fs.sftp.proxy.port", proxy.getPort());
      conf.setEnum("fs.sftp.proxy.type",
              AbstractFTPFileSystem.ProxyType.SOCKS5);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info);
      assertNotNull(channel);
      Session s = channel.getNative().getSession();
      Object o = FieldUtils.readField(s, "proxy", true);
      assertNotNull(o);
      assertTrue("Type of proxy should be SOCKS5", o instanceof ProxySOCKS5);
    } finally {
      proxy.stop();
    }
  }

  @Test
  public void testProxyHTTP() throws Exception {
    LOG.info("testProxyHTTP");
    HttpProxyServer proxy
            = DefaultHttpProxyServer
                    .bootstrap()
                    .withPort(0)
                    .start();

    try {
      Configuration conf = new Configuration(ftpFs.getConf());
      conf.setInt("fs.sftp.proxy.port", proxy.getListenAddress().getPort());
      conf.setEnum("fs.sftp.proxy.type", AbstractFTPFileSystem.ProxyType.HTTP);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info);
      assertNotNull(channel);
      Session s = channel.getNative().getSession();
      Object o = FieldUtils.readField(s, "proxy", true);
      assertNotNull(o);
      assertTrue("Type of proxy should be HTTP", o instanceof ProxyHTTP);
    } finally {
      proxy.stop();
    }
  }
}
