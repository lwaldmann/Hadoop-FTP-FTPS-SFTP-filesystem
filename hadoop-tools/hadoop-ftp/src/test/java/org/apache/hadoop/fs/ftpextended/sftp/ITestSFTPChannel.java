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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import net.sourceforge.jsocks.ProxyServer;
import net.sourceforge.jsocks.server.ServerAuthenticatorNone;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystem;
import org.apache.hadoop.fs.ftpextended.common.AbstractFTPFileSystemTest;
import org.apache.hadoop.fs.ftpextended.common.Channel;
import org.apache.hadoop.fs.ftpextended.common.ConnectionInfo;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.apache.hadoop.fs.ftpextended.contract.FTPContractTestMixin;
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
          = FTPContractTestMixin.getRandomizedTempPath();

//  @Rule
//  public Timeout testTimeout = new Timeout(1 * 60 * 1000);

  private static final Logger LOG = LoggerFactory.getLogger(
          ITestSFTPChannel.class);

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

  private Configuration conf;

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
    conf = new Configuration();
    conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.sftp.host.port", server.getPort());
    conf.setBoolean("fs.sftp.impl.disable.cache", true);
    conf.set("fs.sftp.proxy.host", "localhost");
    conf.setEnum("fs.sftp.proxy.type", AbstractFTPFileSystem.ProxyType.NONE);
  }

  @Test
  public void testProxyNone() throws Exception {
    LOG.info("testProxyNone");
    URI uriInfo = URI.create("sftp://user:password@localhost");
    conf.setEnum("fs.sftp.proxy.type", AbstractFTPFileSystem.ProxyType.NONE);
    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info)) {
      assertNotNull(channel);
      Session s = channel.getNative().getSession();
      Object o = FieldUtils.readField(s, "proxy", true);
      assertNull(o);
    }
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
      URI uriInfo = URI.create("sftp://user:password@localhost");
      LOG.info("Connecting to the port: " + proxy.getPort());
      conf.setInt("fs.sftp.proxy.port", proxy.getPort());
      conf.setEnum("fs.sftp.proxy.type",
              AbstractFTPFileSystem.ProxyType.SOCKS4);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      try (SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info)) {
        assertNotNull(channel);
        Session s = channel.getNative().getSession();
        Object o = FieldUtils.readField(s, "proxy", true);
        assertNotNull(o);
        assertTrue("Type of proxy should be SOCKS4", o instanceof ProxySOCKS4);
      }
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
      URI uriInfo = URI.create("sftp://user:password@localhost");
      LOG.info("Connecting to the port: " + proxy.getPort());
      conf.setInt("fs.sftp.proxy.port", proxy.getPort());
      conf.setEnum("fs.sftp.proxy.type",
              AbstractFTPFileSystem.ProxyType.SOCKS5);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      try (SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info)) {
        assertNotNull(channel);
        Session s = channel.getNative().getSession();
        Object o = FieldUtils.readField(s, "proxy", true);
        assertNotNull(o);
        assertTrue("Type of proxy should be SOCKS5", o instanceof ProxySOCKS5);
      }
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
      URI uriInfo = URI.create("sftp://user:password@localhost");
      conf.setInt("fs.sftp.proxy.port", proxy.getListenAddress().getPort());
      conf.setEnum("fs.sftp.proxy.type", AbstractFTPFileSystem.ProxyType.HTTP);
      ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo,
              conf, 0);
      try (SFTPChannel channel = (SFTPChannel) SFTPChannel.create(info)) {
        assertNotNull(channel);
        Session s = channel.getNative().getSession();
        Object o = FieldUtils.readField(s, "proxy", true);
        assertNotNull(o);
        assertTrue("Type of proxy should be HTTP", o instanceof ProxyHTTP);
      }
    } finally {
      proxy.stop();
    }
  }

  @Test
  public void testPassword() throws Exception {
    LOG.info("testPassword");
    conf.set("fs.sftp.password.localhost.user", "password");
    URI uriInfo = URI.create("sftp://user@localhost");

    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
    }
  }

  @Test
  public void testPasswordJCEKS() throws Exception {
    LOG.info("testPasswordJCEKS");
    AbstractFTPFileSystemTest.setEnv();
    URI uriInfo = URI.create("sftp://user@localhost");
    URL url = this.getClass().getClassLoader().getResource("keystore.jceks");
    conf.set("hadoop.security.credential.provider.path", new URI("jceks", "file",
            url.getPath(), null).toString());
    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
      assertTrue(
              "No proxy used therefore class client class shouldn't be proxied",
              ChannelSftp.class.equals(channel.getNative().getClass()));
    }
  }

  @Test
  public void testCredentialsKeyJCEKS() throws Exception {
    LOG.info("testCredentialsKeyJCEKS");
    AbstractFTPFileSystemTest.setEnv();
    URI uriInfo = URI.create("sftp://user1@localhost");
    URL url = this.getClass().getClassLoader().getResource("keystore.jceks");
    conf.set("hadoop.security.credential.provider.path", new URI("jceks", "file",
            url.getPath(), null).toString());
    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
    }
  }

  @Test
  public void testCredentialsKeyFilePassphraseJCEKS() throws Exception {
    LOG.info("testCredentialsKeyFilePassphraseJCEKS");
    AbstractFTPFileSystemTest.setEnv();
    URI keyUri = this.getClass().getClassLoader()
            .getResource("test-user-pass").toURI();
    conf.set("fs.sftp.key.file.localhost.user2", keyUri.toString());
    URL url = this.getClass().getClassLoader().getResource("keystore.jceks");
    conf.set("hadoop.security.credential.provider.path", new URI("jceks", "file",
            url.getPath(), null).toString());
    URI uriInfo = URI.create("sftp://user2@localhost");

    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
    }
  }

  @Test
  public void testCredentialsKeyFile() throws Exception {
    LOG.info("testCredentialsKeyFile");
    URI keyUri = this.getClass().getClassLoader()
            .getResource("test-user").toURI();
    conf.set("fs.sftp.key.file.localhost.user", keyUri.toString());
    URI uriInfo = URI.create("sftp://user@localhost");

    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
    }
  }

  @Test
  public void testCredentialsKeyFilePassphrase() throws Exception {
    LOG.info("testCredentialsKeyFile");
    URI keyUri = this.getClass().getClassLoader()
            .getResource("test-user-pass").toURI();
    conf.set("fs.sftp.key.file.localhost.user", keyUri.toString());
    conf.set("fs.sftp.key.passphrase.localhost.user", "passphrase");
    URI uriInfo = URI.create("sftp://user@localhost");

    ConnectionInfo info = new ConnectionInfo(SFTPChannel::create, uriInfo, conf,
            0);
    try (Channel channel = SFTPChannel.create(info)) {
      assertNotNull(channel);
    }
  }
}
