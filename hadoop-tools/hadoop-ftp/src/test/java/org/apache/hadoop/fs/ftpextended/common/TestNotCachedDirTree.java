/*
 * Copyright 2017 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ftpextended.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.LoggerFactory;

/**
 *  Test not cached directories.
 */
public class TestNotCachedDirTree {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          TestNotCachedDirTree.class);

  private DirTree tree;
  private Channel channel;
  private static final String URI_UP_HOST = "ftp://user:password@localhost";
  private Configuration conf;
  private ConnectionInfo info;

  @Before
  public void setup() throws URISyntaxException, IOException {
    conf = new Configuration();
    tree = new NotCachedDirTree();
    info = new ConnectionInfo(DummyChannel::new, new URI(URI_UP_HOST), conf,
            21);
    channel = info.getConnectionSupplier().apply(info);
  }

  /**
   * Test of addNode method, of class NotCachedDirTree.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAddNode() throws Exception {
    LOG.info("addNode");
    Path p = new Path("/test/file.txt");
    DirTree.INode result = tree.addNode(channel, p);
    assertNotNull("Node should be created even for not cached file system",
            result);
    assertEquals(p, result.getStatus().getPath());
    assertFalse("Not cached file system is always incomplete",
            result.isCompleted());
    Collection c = result.getChildren(channel);
    assertNotNull("List should be always returned", c);
    assertEquals("List should be always empty", 0, c.size());
  }

  /**
   * Test of findNode method, of class NotCachedDirTree.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testFindNode() throws Exception {
    LOG.info("findNode");
    Path p = new Path("/test/file.txt");
    DirTree.INode result = tree.findNode(p);
    assertNull("Not added to the tree", result);
    tree.addNode(channel, p);
    result = tree.findNode(p);
    assertNull("Not cached file system won't found item even after adding",
            result);
  }
}
