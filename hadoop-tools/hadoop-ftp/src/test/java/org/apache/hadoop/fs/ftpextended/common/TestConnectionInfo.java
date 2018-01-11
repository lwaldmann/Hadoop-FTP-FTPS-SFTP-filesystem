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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 *
 */
public class TestConnectionInfo {

  private static final Logger LOG = LoggerFactory.getLogger(
          TestConnectionInfo.class);

  public static final String URI_UP_HOST = "ftp://user:password@localhost";
  public static final String URI_U_HOST = "ftp://user@localhost";
  public static final String URI_HOST = "ftp://localhost";
  public static final String URI_EMPTY = "ftp:///";
  public static final String URI_U_OTHER_HOST = "ftp://user@other";
  public static final String URI_HOST_PORT = "ftp://localhost:1234";
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration(false);
  }

  /**
   * Test of equals method, of class ConnectionInfo.
   */
  @Test
  public void testEquals() throws URISyntaxException, IOException {
    LOG.info("test equals");
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertTrue("Equals method should return true on itself",
            info1.equals(info1));

   // two same uris
    ConnectionInfo info2 = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertTrue("Equals method should return true for the same config params",
            info1.equals(info2));

   // different target server
    ConnectionInfo info3 = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_OTHER_HOST), conf, 21);
    assertFalse(
            "Equals method should return false for different config params" +
                    " - URI",
            info1.equals(info3));

   // the same server but different port
    conf.setInt("fs.ftp.host.port", 23);
    ConnectionInfo info4 = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertFalse(
            "Equals method should return false for different config params" +
                    " - port",
            info1.equals(info4));

   // the same server but different user
    conf.set("fs.ftp.user.localhost", "test");
    conf.unset("fs.ftp.host.port");
    ConnectionInfo info5 = new ConnectionInfo(DummyChannel::new, new URI(
            URI_HOST), conf, 21);
    assertFalse(
            "Equals method should return false for different config params" +
                    " - user",
            info1.equals(info5));
  }

  /**
   * Test of getProxyType method, of class ConnectionInfo.
   */
  @Test
  public void testGetProxyType() throws URISyntaxException, IOException {
    LOG.info("getProxyType");

   // default proxy type
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals(AbstractFTPFileSystem.ProxyType.NONE, info.getProxyType());

   // NONE proxy type
    conf.set("fs.ftp.proxy.type", "NONE");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(AbstractFTPFileSystem.ProxyType.NONE, info.getProxyType());

    // HTTP proxy type
    conf.set("fs.ftp.proxy.type", "HTTP");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(AbstractFTPFileSystem.ProxyType.HTTP, info.getProxyType());

   // SOCKS4 proxy type
    conf.set("fs.ftp.proxy.type", "SOCKS4");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(AbstractFTPFileSystem.ProxyType.SOCKS4, info.getProxyType());

   // SOCKS5 proxy type
    conf.set("fs.ftp.proxy.type", "SOCKS5");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(AbstractFTPFileSystem.ProxyType.SOCKS5, info.getProxyType());
  }

  /**
   * Test of getProxyHost method, of class ConnectionInfo.
   */
  @Test
  public void testGetProxyHost() throws URISyntaxException, IOException {
    LOG.info("getProxyHost");

   // default proxy
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals(null, info.getProxyHost());

   // soem proxy
    conf.set("fs.ftp.proxy.host", "some");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals("some", info.getProxyHost());
  }

  /**
   * Test of getProxyPort method, of class ConnectionInfo.
   */
  @Test
  public void testGetProxyPort() throws URISyntaxException, IOException {
    LOG.info("getProxyPort");

   // default proxy
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals(-1, info.getProxyPort());

   // NONE proxy type
    conf.set("fs.ftp.proxy.type", "NONE");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(-1, info.getProxyPort());

    // HTTP proxy type
    conf.set("fs.ftp.proxy.type", "HTTP");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(8080, info.getProxyPort());

   // SOCKS4 proxy type
    conf.set("fs.ftp.proxy.type", "SOCKS4");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(1080, info.getProxyPort());

   // SOCKS5 proxy type
    conf.set("fs.ftp.proxy.type", "SOCKS5");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(1080, info.getProxyPort());
  }

  /**
   * Test of getProxyUser method, of class ConnectionInfo.
   */
  @Test
  public void testGetProxyUser() throws URISyntaxException, IOException {
    LOG.info("getProxyUser");

   // default proxy
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals(null, info.getProxyUser());

   // some user
    conf.set("fs.ftp.proxy.user", "some");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals("some", info.getProxyUser());
  }

  /**
   * Test of getProxyPassword method, of class ConnectionInfo.
   */
  @Test
  public void testGetProxyPassword() throws URISyntaxException, IOException {
    LOG.info("getProxyPassword");

   // default proxy
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals(null, info.getProxyPassword());

   // some user
    conf.set("fs.ftp.proxy.password", "some");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals("some", info.getProxyPassword());
  }

  /**
   * Test of getFtpHost method, of class ConnectionInfo.
   */
  @Test
  public void testGetFtpHost() throws URISyntaxException, IOException {
    LOG.info("getFtpHost");

    // host from uri
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 21);
    assertEquals("localhost", info.getFtpHost());

    // host from conf
    conf.set("fs.ftp.host", "other");
    conf.set("fs.ftp.user.other", "another");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_EMPTY), conf, 21);
    assertEquals("other", info.getFtpHost());
  }

  /**
   * Test of getFtpHost method, of class ConnectionInfo.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetFtpHostEmpty() throws URISyntaxException, IOException {
    LOG.info("getFtpHostEmpty");

    ConnectionInfo info = new ConnectionInfo(DummyChannel::new,
            new URI(URI_HOST), conf, 21);
  }

  /**
   * Test of getFtpPort method, of class ConnectionInfo.
   */
  @Test
  public void testGetFtpPort() throws URISyntaxException, IOException {
    LOG.info("getFtpPort");
    // host from uri
    conf.unset("fs.ftp.host.port");
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_UP_HOST), conf, 33);
    assertEquals(33, info.getFtpPort());

   // from config
    conf.setInt("fs.ftp.host.port", 44);
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    assertEquals(44, info.getFtpPort());

   // from URI
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_HOST_PORT), conf,
            21);
    assertEquals(1234, info.getFtpPort());
  }

  /**
   * Test of getFtpUser method, of class ConnectionInfo.
   */
  @Test
  public void testGetFtpUser() throws URISyntaxException, IOException {
    LOG.info("getFtpUser");

   // from conf
    conf.set("fs.ftp.host", "other");
    conf.set("fs.ftp.user.other", "another");
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_EMPTY), conf, 21);
    assertEquals("another", info.getFtpUser());

   // form uri
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            33);
    assertEquals("user", info.getFtpUser());
  }

  /**
   * Test of getFtpUser method, of class ConnectionInfo.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetFtpUserEmpty() throws URISyntaxException, IOException {
    LOG.info("getFtpUserEmpty");

    ConnectionInfo info = new ConnectionInfo(DummyChannel::new,
            new URI(URI_HOST), conf, 21);
  }

  /**
   * Test of getFtpPassword method, of class ConnectionInfo.
   */
  @Test
  public void testGetFtpPassword() throws URISyntaxException, IOException {
    LOG.info("getFtpPassword");

   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(null, info.getFtpPassword());

   // from conf
    conf.set("fs.ftp.password.localhost.user", "test");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertEquals("test", info.getFtpPassword());

   // form uri
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            33);
    assertEquals("password", info.getFtpPassword());
  }

  /**
   * Test of getMaxConnections method, of class ConnectionInfo.
   */
  @Test
  public void testGetMaxConnections() throws URISyntaxException, IOException {
    LOG.info("getMaxConnections");

   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(ConnectionInfo.DEFAULT_MAX_CONNECTION,
            info.getMaxConnections());

   // from conf
    conf.set("fs.ftp.connections.max", "10");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertEquals(10, info.getMaxConnections());
  }

  /**
   * Test of getKeepAlivePeriod method, of class ConnectionInfo.
   */
  @Test
  public void testGetKeepAlivePeriod() throws URISyntaxException, IOException {
    LOG.info("getKeepAlivePeriod");

   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(ConnectionInfo.DEFAULT_KEEPALIVE_PERIOD,
            info.getKeepAlivePeriod());

   // from conf
    conf.set("fs.ftp.keepalive.period", "10");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertEquals(10, info.getKeepAlivePeriod());
  }

  /**
   * Test of isCacheDirectories method, of class ConnectionInfo.
   */
  @Test
  public void testIsCacheDirectories() throws URISyntaxException, IOException {
    LOG.info("isCacheDirectories");

   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(ConnectionInfo.DEFAULT_CACHE_DIRECTORIES,
            info.isCacheDirectories());

   // from conf
    conf.set("fs.ftp.cache." + new URI(URI_U_HOST).getHost(), "true");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertTrue("Cache directories flag should be set",
            info.isCacheDirectories());
  }

  /**
   * Test of isUseKeepAlive method, of class ConnectionInfo.
   */
  @Test
  public void testIsUseKeepAlive() throws URISyntaxException, IOException {
    LOG.info("isUseKeepAlive");

   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(ConnectionInfo.DEFAULT_USE_KEEPALIVE, info.isUseKeepAlive());

   // from conf
    conf.set("fs.ftp.use.keepalive", "true");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertTrue("KeepAlive should be set to true", info.isUseKeepAlive());

    conf.set("fs.ftp.use.keepalive", "false");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertFalse("KeepAlive should be set to false", info.isUseKeepAlive());
  }

  /**
   * Test of getGlobType method, of class ConnectionInfo.
   */
  @Test
  public void testGetGlobType() throws URISyntaxException, IOException {
    LOG.info("getGlobType");
   // default
    ConnectionInfo info = new ConnectionInfo(DummyChannel::new, new URI(
            URI_U_HOST), conf, 21);
    assertEquals(AbstractFTPFileSystem.GlobType.UNIX, info.getGlobType());

   // from conf
    conf.set("fs.ftp.glob.type", "REGEXP");
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_U_HOST), conf, 21);
    assertEquals(AbstractFTPFileSystem.GlobType.REGEXP, info.getGlobType());
  }
}
