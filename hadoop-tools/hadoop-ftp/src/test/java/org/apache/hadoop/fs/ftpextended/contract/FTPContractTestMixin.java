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
import java.util.Arrays;
import java.util.Collection;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.test.GenericTestUtils;

/**
 * Use as common for all type of contracts.
 */
public interface FTPContractTestMixin {

  Path TEST_ROOT = new Path(CommonContract.createTestPath(new Path("/")),
          GenericTestUtils.getRandomizedTempPath());

  static Collection<Object[]> getData() {
    Object[][] data = {{true, "ftp"}, {true, "sftp"}, {true, "ftps"},
        {false, "ftp"}, {false, "sftp"}, {false, "ftps"}};
    return Arrays.asList(data);
  }

  default AbstractFSContract setupContract(Configuration conf, boolean cache,
          String schema, String method) {

    try {
      conf.setBoolean("fs." + schema + ".cache.directories", cache);
      FileSystem localFs = FileSystem.getLocal(new Configuration());
      localFs.mkdirs(TEST_ROOT);
      localFs.setWorkingDirectory(TEST_ROOT);
      localFs.setWriteChecksum(false);
      localFs.enableSymlinks();
      return new CommonContract(conf, schema, TEST_ROOT.toString(), method);
    } catch (IOException | FtpException ex) {
      throw new IllegalStateException(ex);
    }
  }

  default void cleanUp(AbstractFSContract contract) throws IOException {
    contract.getTestFileSystem().close();
    ((CommonContract)contract).stopServer();
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    if (localFs.exists(TEST_ROOT)) {
      localFs.delete(TEST_ROOT, true);
    }
  }
}
