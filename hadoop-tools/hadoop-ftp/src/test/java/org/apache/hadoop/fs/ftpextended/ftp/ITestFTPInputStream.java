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
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftpextended.common.Channel;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.ftpextended.ftp.ITestFTPFileSystem.FTP_URI;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.rules.Timeout;

/**
 * Test functionality of retrieving data fro ftp server.
 */
public class ITestFTPInputStream {

  private static final String TEST_ROOT_DIR
          = GenericTestUtils.getRandomizedTempPath();
  private static final Logger LOG = LoggerFactory.getLogger(
          ITestFTPInputStream.class);
  private static FileSystem localFs = null;
  private FTPFileSystem ftpFs = null;

  private final Path localDir = new Path(getClass().getSimpleName());
  private final Configuration conf = new Configuration();

  private static FTPServer server;

  @Rule
  public TestName name = new TestName();

  @Rule
  public Timeout testTimeout = new Timeout(1 * 60 * 1000);

  @BeforeClass
  public static void setClass() throws FtpException, IOException {
    FTPChannel.timeoutInSeconds = 1;
    FTPChannel.keepAliveMultiplier = 1;
    localFs = FileSystem.getLocal(new Configuration());
    Path root = new Path(TEST_ROOT_DIR);
    localFs.mkdirs(root);
    localFs.setWorkingDirectory(root);
    localFs.setWriteChecksum(false);
    localFs.enableSymlinks();
    server = new FTPServer(TEST_ROOT_DIR);
  }

  @AfterClass
  public static void cleanClass() throws IOException {
    server.stop();
    localFs.delete(new Path(TEST_ROOT_DIR), true);
  }

  @Before
  public void setup() throws FtpException, IOException {
    if (localFs.exists(localDir)) {
      localFs.delete(localDir, true);
    }
    localFs.mkdirs(localDir);
    conf.setClass("fs.ftp.impl", FTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.ftp.host.port", server.getPort());
    ftpFs = (FTPFileSystem) FileSystem.get(FTP_URI, conf);
  }

  @After
  public void teardown() {
    if (ftpFs != null) {
      try {
        ftpFs.close();
      } catch (IOException e) {
        // ignore
      }
    }
    ftpFs = null;
    if (localFs != null) {
      try {
        localFs.delete(localDir, true);
        localFs.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  /**
   * Class used to simulate connection interruptions.
   */
  private class BreakableStream extends InputStream {

    private final int breakOn;
    private int counter = 0;
    private final byte[] data;
    private byte[] newData = null;

    BreakableStream(byte[] data, int c) {
      breakOn = c;
      if (c >= data.length) {
        throw new IllegalArgumentException("Break set after data length");
      }
      this.data = data;
    }

    void setNewData(byte[] newData) {
      this.newData = newData;
    }

    @Override
    public int read() throws IOException {
      if (counter == breakOn) {
        if (newData != null) {
          writeDataset(localFs, new Path(localDir,
                  name.getMethodName().toLowerCase()), newData, newData.length,
                  1024, true);
        }
        return -1;
      }

      return data[counter++];
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (counter < breakOn) {
        int i;
        for (i = 0; i < breakOn && i < len;) {
          b[off + i++] = data[counter++];
        }
        if (newData != null) {
          writeDataset(localFs, new Path(localDir,
                  name.getMethodName().toLowerCase()), newData, newData.length,
                  1024, true);
        }
        return i;
      } else {
        return -1;
      }
    }
  }

  @Test
  public void testReconnectByteRead() throws IOException {
    LOG.info("test read by byte");
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yaks", true);
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
   // try reconnect on first byte
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 0),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByByte(fis, data.length));
    }

   // try reconnect in the middle
    ch = ftpFs.connect();
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 2),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByByte(fis, data.length));
    }

   // try reconnect at last byte
    ch = ftpFs.connect();
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 3),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByByte(fis, data.length));
    }
  }

  @Test
  public void testMakeBiggerLength() throws IOException {
    LOG.info("test file update to bigger");
    byte[] newData = "yakstaks".getBytes();
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yaks", true);
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
   // try reconnect on first byte
    BreakableStream bs = new BreakableStream(data, 2);
    try (FTPInputStream fis = new FTPInputStream(bs, (FTPChannel) ch, fs,
            new FileSystem.Statistics("test"))) {
      bs.setNewData(newData);
      assertArrayEquals(newData, readStreamByArray(fis, 1024));
    }
  }

  @Test
  public void testMakeShorterLength() throws IOException {
    LOG.info("test file update to smaller");
    byte[] newData = "yaks".getBytes();
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yakstaks", true);
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
   // try reconnect on first byte
    BreakableStream bs = new BreakableStream(data, 2);
    try (FTPInputStream fis = new FTPInputStream(bs, (FTPChannel) ch, fs,
            new FileSystem.Statistics("test"))) {
      bs.setNewData(newData);
      assertArrayEquals(newData, readStreamByArray(fis, 1024));
    }
  }

  @Test
  public void testKeepAlive() throws IOException, InterruptedException {
    LOG.info("test sending keep alive");
    conf.setBoolean("fs.ftp.use.keepalive", true);
    conf.setInt("fs.ftp.keepalive.period", 1);
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yakstaks", true);
    FTPChannel.keepAliveMultiplier = 1;
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
    boolean[] b = {false};
    server.getServerFactory().getFtplets().put("Test+" + name.getMethodName(),
            new DefaultFtplet() {
        @Override
        public FtpletResult beforeCommand(FtpSession session,
                FtpRequest request) throws FtpException, IOException {
          if ("NOOP".equals(request.getCommand())) {
            b[0] = true;
            try {
              Thread.sleep(2000);
            } catch (InterruptedException ex) {
              LOG.info("Not expected", ex);
            }
          }

          return super.beforeCommand(session, request);
        }

      });
    try {
     // Noop should arrive after 1sec;
      BreakableStream bs = new BreakableStream(data, 2);
      try (FTPInputStream fis = new FTPInputStream(bs, (FTPChannel) ch, fs,
              new FileSystem.Statistics("test"))) {
        try {
          Thread.sleep(1500);
        } catch (InterruptedException ex) {
          LOG.error(ch.getConnectionInfo().logWithInfo("Unexpected exception"),
                  ex);
        }
        int i;
        do {
          i = fis.read();
        } while (i != -1);
      }
      assertEquals(true, b[0]);

      b[0] = false;
      ch = ftpFs.connect();
     // Noop should arrive after 1sec so waiting shorter will not set the flag;
      bs = new BreakableStream(data, 2);
      try (FTPInputStream fis = new FTPInputStream(bs, (FTPChannel) ch, fs,
              new FileSystem.Statistics("test"))) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {
          LOG.error(ch.getConnectionInfo().logWithInfo("Unexpected exception"),
                  ex);
        }
        int i;
        do {
          i = fis.read();
        } while (i != -1);
      }
      assertEquals(false, b[0]);
    } finally {
      server.getServerFactory().
              getFtplets().remove("Test+" + name.getMethodName());
    }
  }

  @Test
  public void testReconnectArrayRead() throws IOException {
    LOG.info("test reconnect read array");
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yaks", true);
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
   // try reconnect on first byte
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 0),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByArray(fis, data.length));
    }

   // try reconnect in the middle
    ch = ftpFs.connect();
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 2),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByArray(fis, data.length));
    }

   // try reconnect at last byte
    ch = ftpFs.connect();
    try (FTPInputStream fis = new FTPInputStream(new BreakableStream(data, 3),
            (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByArray(fis, data.length));
    }
  }

  @Test
  public void testReadTimeout() throws IOException {
    LOG.info("test reconnect after socket timeout");
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yaks", true);

    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
    try (FTPInputStream fis = new FTPInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        throw new SocketTimeoutException();
      }

    }, (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByByte(fis, data.length));
    }

    ch = ftpFs.connect();
    try (FTPInputStream fis = new FTPInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        throw new SocketTimeoutException();
      }
    }, (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      assertArrayEquals(data, readStreamByArray(fis, data.length));
    }
  }

  @Test(expected = java.io.IOException.class)
  public void testReadByteException() throws IOException {
    LOG.info("test unrecoverable exception reading by bytes");
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    writeTextFile(localFs, file, "yaks", true);

    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
    try (FTPInputStream fis = new FTPInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IOException();
      }

    }, (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      fis.read();
    }
  }

  @Test(expected = java.io.IOException.class)
  public void testReadArrayException() throws IOException {
    LOG.info("test unrecoverable exception reading by array");
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    byte[] data = writeTextFile(localFs, file, "yaks", true);
    Channel ch = ftpFs.connect();
    FileStatus fs = ch.getFileStatus(file, new HashSet<>());
    try (FTPInputStream fis = new FTPInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        throw new IOException();
      }

    }, (FTPChannel) ch, fs, new FileSystem.Statistics("test"))) {
      byte[] b = new byte[data.length];
      fis.read(b, 0, data.length);
    }
  }

  private byte[] readStreamByByte(FTPInputStream fis, int length) throws
          IOException {
    byte[] b = new byte[length];
    int c = 0;
    int r;
    while ((r = fis.read()) != -1) {
      if (r != 0) {
        b[c++] = (byte) r;
      }
    }
    return b;
  }

  private byte[] readStreamByArray(FTPInputStream fis, int length) throws
          IOException {
    byte[] b = new byte[length];
    int c = 0;
    int r = 0;
    int left = length;
    do {
      c += r;
      if (c == length) {
        break;
      }
      r = fis.read(b, c, length - c);
    } while (r != -1);
    byte[] resArray = new byte[c];
    System.arraycopy(b, 0, resArray, 0, c);
    return resArray;
  }

}
