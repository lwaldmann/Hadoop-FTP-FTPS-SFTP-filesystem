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
 * Tests of connection pool.
 */
public class TestConnectionPool {

  private static final Logger LOG = LoggerFactory.getLogger(
          TestConnectionPool.class);

  private final Configuration conf = new Configuration();

  @Before
  public void setup() {
    ConnectionPool.resetPool();
  }

  /**
   * Test of getConnectionPool method, of class ConnectionPool.
   */
  @Test
  public void testGetConnectionPool() {
    LOG.info("getConnectionPool");
    int maxConnection = 20;
    ConnectionPool result = ConnectionPool.getConnectionPool(maxConnection);
    assertEquals(20, result.getMaxConnection());
  }

  /**
   * Test of shutdown method, of class ConnectionPool.
   */
  @Test
  public void testShutdown() throws URISyntaxException, IOException {
    LOG.info("shutdown");
    ConnectionPool instance = ConnectionPool.getConnectionPool(2);
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test1"), conf, 0);
    ConnectionInfo info2 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test2"), conf, 0);
    Channel ch1 = instance.connect(info1);
    Channel ch2 = instance.connect(info2);
    Channel ch3 = instance.connect(info1);
    Channel ch4 = instance.connect(info2);
    assertTrue("Connection should be established", ch1.isConnected());
    assertTrue("Connection should be established", ch2.isConnected());
    assertTrue("Connection should be established", ch3.isConnected());
    assertTrue("Connection should be established", ch4.isConnected());
    instance.disconnect(ch3, false);
    instance.shutdown(info1);
    instance.shutdown(info2);
   // return to the pool - items should be destroyed because FS is closed
    instance.disconnect(ch1, false);
    instance.disconnect(ch2, false);
    instance.disconnect(ch4, false);
    assertFalse("Connection should be closed", ch1.isConnected());
    assertFalse("Connection should be closed", ch2.isConnected());
    assertFalse("Connection should be closed", ch3.isConnected());
    assertFalse("Connection should be closed", ch4.isConnected());
  }

  @Test(expected = IOException.class)
  public void testGetChannelFromClosedFS() throws URISyntaxException,
          IOException {
    LOG.info("getChannelFromClosedFS");
    ConnectionPool instance = ConnectionPool.getConnectionPool();
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test1"), conf, 0);
    ConnectionInfo info2 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test2"), conf, 0);
    Channel ch1 = instance.connect(info1);
    assertNotNull(ch1);
    instance.shutdown(info1);
    Channel ch2 = instance.connect(info2);
    assertTrue("This FS shouldn't be closed and connection should succeed",
            ch2.isPooled());

   // Check if we can get some new channel, should throw an exception
    instance.connect(info1);
  }

  /**
   * Test of setMaxConnection method, of class ConnectionPool.
   */
  @Test
  public void testSetMaxConnection() throws URISyntaxException, IOException {
    LOG.info("setMaxConnection");
    ConnectionPool instance = ConnectionPool.getConnectionPool();
    instance.setMaxConnection(2);
    assertEquals(2, instance.getMaxConnection());
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test1"), conf, 0);
    Channel ch1 = instance.connect(info1);
    Channel ch2 = instance.connect(info1);
    assertTrue(
            "Max connection not yet reached and therefore " +
                    "connection should be pooled",
            ch2.isPooled());
    instance.disconnect(ch2, false);
    Channel result = instance.connect(info1);
    assertEquals(ch2, result);
    Channel ch3 = instance.connect(info1);
    assertFalse(
            "Max connection reached and therefore " +
                    "connection should not be pooled",
            ch3.isPooled());
   // channel will be thrown away as it is over the limit
    instance.disconnect(ch3, false);
    result = instance.connect(info1);
    assertNotEquals(ch3, result);
    assertFalse(
            "Max connection still reached and therefore " +
                    "connection should not be pooled",
            result.isPooled());
  }

  /**
   * Test of connect method, of class ConnectionPool.
   */
  @Test
  public void testConnectAndDisconnect()
          throws URISyntaxException, IOException {
    LOG.info("connect");
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test1"), conf, 0);
    ConnectionPool instance = ConnectionPool.getConnectionPool(2);
    instance.init(info1);
    Channel result1 = instance.connect(info1);
    assertNotNull(result1);
    Channel result2 = instance.connect(info1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);

   // disconnect and return to the pool
    instance.disconnect(result2, false);
    Channel result3 = instance.connect(info1);
    assertEquals(result2, result3);

   // Disconnect and not return to the pool
    instance.disconnect(result2, true);
    result3 = instance.connect(info1);
    assertNotEquals(result2, result3);
    assertFalse(
            "Connection should be forcible disconnected but is still connected",
            result2.isConnected());

   // Get over connection limit
    Channel result4 = instance.connect(info1);
    assertNotEquals(result3, result4);
    instance.disconnect(result4, false);
    Channel result5 = instance.connect(info1);
    assertNotEquals(result4, result5);
  }

  @Test
  public void testGetClosedConnectionFromPool() throws URISyntaxException,
          IOException {
    LOG.info("GetClosedConnectionFromPool");
    ConnectionInfo info1 = new ConnectionInfo(DummyChannel::new, new URI(
            "http://test:test@test1"), conf, 0);
    ConnectionPool instance = ConnectionPool.getConnectionPool(2);
    Channel ch1 = instance.connect(info1);
    instance.disconnect(ch1, false);
    ch1.close();
    Channel ch2 = instance.connect(info1);
    assertNotEquals(ch1, ch2);
  }
}
