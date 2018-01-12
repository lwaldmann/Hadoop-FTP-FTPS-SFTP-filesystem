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
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Test rename contract.
 */
@RunWith(Parameterized.class)
public class ITestContractRename extends AbstractContractRenameTest implements
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
    return setupContract(conf, cache, schema, GenericTestUtils.getMethodName());
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanUp(getContract());
  }
}
