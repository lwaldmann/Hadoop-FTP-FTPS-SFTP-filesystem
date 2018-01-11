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

import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Test distcp contract.
 */
@RunWith(Parameterized.class)
public class ITestContractDistCp extends AbstractContractDistCpTest implements
        FTPContractTestMixin {

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
    testName = replaceName;
    return setupContract(conf, cache, schema, replaceName.getMethodName());
  }
  /*
   * Parametrized tests put square brackets to the test name which causes
   * problems in glob processing. Let's replace brackets with something more
   * innocent
   */
  @Rule
  public TestName replaceName = new TestName() {
    @Override
    public String getMethodName() {
      return super.getMethodName().replaceAll("([\\[\\]])", "-");
    }
  };

  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanUp(getContract());
  }

  @AfterClass
  public static void removeLocalDir() throws Exception {
    Path testSubDir = new Path(ITestContractDistCp.class.getSimpleName());
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path localDir = localFs.makeQualified(new Path(new Path(
        GenericTestUtils.getTestDir().toURI()), testSubDir));
    localFs.delete(localDir, true);
  }

}
