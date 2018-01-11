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
package org.apache.hadoop.fs.ftpextended.contract;

import java.io.IOException;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.apache.hadoop.fs.ftpextended.ftp.FTPFileSystem;
import org.apache.hadoop.fs.ftpextended.ftp.FTPSServer;
import org.apache.hadoop.fs.ftpextended.ftp.FTPServer;
import org.apache.hadoop.fs.ftpextended.sftp.SFTPFileSystem;
import org.apache.hadoop.fs.ftpextended.sftp.SFTPServer;

/**
 * The contract of FTP/SFTP.
 */
public class CommonContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "contract/params.xml";
  public static final String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";
  private static final String TEST_SERVER = "fs.contract.use.internal.";
  private final String schema;
  private final Path root;
  private Server server;

  public CommonContract(Configuration conf, String schema, String root,
          String method) throws
          IOException, FtpException {
    super(conf);
   // insert the base features
    addConfResource(CONTRACT_XML);
    if (conf.getBoolean(TEST_SERVER + schema + "server", false)) {
      switch (schema) {
      case "ftp":
        server = new FTPServer(root);
        break;
      case "ftps":
        server = new FTPSServer(root);
        break;
      case "sftp":
        server = new SFTPServer(root);
        break;
      default:
        throw new IOException("Unknows schema" + schema);
      }
      conf.setInt("fs." + schema + ".host.port",
                server.getPort());
      conf.set("fs.contract.test.fs." + schema, schema +
              "://user:password@localhost/");
    }
    if ("ftp".equals(schema) || "ftps".equals(schema)) {
      conf.setClass("fs." + schema + ".impl", FTPFileSystem.class,
              FileSystem.class);
    } else {
      conf.setClass("fs." + schema + ".impl", SFTPFileSystem.class,
              FileSystem.class);
    }
    conf.setBoolean("fs." + schema + ".impl.disable.cache", true);
    this.schema = schema;
    this.root = new Path(method);
  }

  @Override
  public String getScheme() {
    return schema;
  }

  @Override
  public Path getTestPath() {
    return root;
  }

  public void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Create a test path, using the value of {@link TEST_UNIQUE_FORK_ID} if it is
   * set.
   *
   * @param defVal default value
   * @return a path
   */
  public static Path createTestPath(Path defVal) {
    String testUniqueForkId = System.getProperty(TEST_UNIQUE_FORK_ID);
    return testUniqueForkId == null ? defVal
            : new Path(testUniqueForkId, "test");
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return super.getTestFileSystem();
  }
}
