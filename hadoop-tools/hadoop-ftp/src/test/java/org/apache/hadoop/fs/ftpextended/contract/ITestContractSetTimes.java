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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSetTimesTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Test delete contract.
 */
@RunWith(Parameterized.class)
public class ITestContractSetTimes extends AbstractContractSetTimesTest
        implements FTPContractTestMixin {

  @SuppressWarnings("checkstyle:visibilitymodifier")
  @Parameterized.Parameter(0)
  public boolean cache;

  @SuppressWarnings("checkstyle:visibilitymodifier")
  @Parameterized.Parameter(1)
  public String schema;

  @Parameterized.Parameters(name = "Schema {1} - Cache enabled {0}")
  public static Collection<Object[]> data() {
    return FTPContractTestMixin.getData();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) throws
          IllegalStateException {
    return setupContract(conf, cache, schema, this.methodName.getMethodName());
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanUp(getContract());
  }

  @Test
  public void testSetTimesFile() throws Throwable {
    try {
      long time = System.currentTimeMillis()+1000*60*60;
      Path p = createDirWithEmptySubFolder();
      getFileSystem().setTimes(p, time, time);
      FileStatus fs = getFileSystem().getFileStatus(p);
      DateFormat tm = new SimpleDateFormat("yyyyMMddHHmm");
      tm.setTimeZone(TimeZone.getTimeZone("GMT"));
      assertEquals("Time not set properly", tm.format(new Date(time)),
              tm.format(new Date(fs.getModificationTime())));
      //got here: trouble
    } catch (FileNotFoundException e) {
      fail("Not able to set time");
    }
  }

  private Path createDirWithEmptySubFolder() throws IOException {
    // remove the test directory
    FileSystem fs = getFileSystem();
    Path path = getContract().getTestPath();
    mkdirs(path);
    // create a - non-qualified - Path for a subdir
    Path file = path.suffix('/' + this.methodName.getMethodName());
    ContractTestUtils.touch(fs, file);
    return file;
  }
}
