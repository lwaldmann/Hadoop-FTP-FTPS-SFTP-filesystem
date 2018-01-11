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

import java.io.FileNotFoundException;
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
 * Class for testing cached directory tree.
 */
public class TestCachedDirTree {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          TestCachedDirTree.class);

  private DirTree tree;
  private Channel channel;
  private static final String URI_UP_HOST = "ftp://user:password@localhost";
  private Configuration conf;
  private ConnectionInfo info;

  @Before
  public void setup() throws URISyntaxException, IOException {
    conf = new Configuration();
    tree = new CachedDirTree(new URI(URI_UP_HOST));
    info = new ConnectionInfo(DummyChannel::new,
            new URI(URI_UP_HOST), conf, 21);
    channel = info.getConnectionSupplier().apply(info);
  }

  /**
   * Test of addNode method, of class CachedDirTree.
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
    assertTrue("File is always completed", result.isCompleted());
    Collection c = result.getChildren(channel);
    assertNotNull("List should be always returned", c);
    assertEquals("List should be always empty for file", 0, c.size());
  }

  /**
   * Test of findNode method, of class CachedDirTree.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testFindNode() throws Exception {
    LOG.info("findNode");
    assertNotNull("Root node should be always found", tree.findNode(
            new Path("/")));
    Path p = new Path("/test/file.txt");
    DirTree.INode result = tree.findNode(p);
    assertNull("Not added to the tree", result);
    tree.addNode(channel, p);
    result = tree.findNode(p);
    assertNotNull("Path was added and should be found", result);
    result = tree.findNode(p.getParent());
    assertNotNull("Even parent node should be found", result);
    assertTrue("Parent directory should be marked completed",
            result.isCompleted());
  }

  /**
   * Test of removeNode method, of class CachedDirTree.
   *
   * @throws java.lang.Exception
   */
  @Test(expected = FileNotFoundException.class)
  public void testRemoveNode() throws Exception {
    LOG.info("findNode");
    assertNotNull("Root node should be always found", tree.findNode(
            new Path("/")));
    Path p = new Path("/test/file.txt");
    assertFalse("Not added path can't be deleted from cache",
            tree.removeNode(p));
    assertFalse("Root can't be deleted from cache", tree.removeNode(
            new Path("/")));
    tree.addNode(channel, p);
    assertTrue("Added node should be deletable", tree.removeNode(p));
    tree.addNode(channel, p);
    assertTrue("Added node parent should be deletable", tree.removeNode(
            p.getParent()));
   // Should throw FileNotFoundException as
    tree.findNode(p);
  }
}
