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
package org.apache.hadoop.fs.ftpextended.common;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Factory for creating pool objects.
 */
class ChannelObjectFactory extends
        BaseKeyedPooledObjectFactory<ConnectionInfo, Channel> {

  @Override
  public Channel create(ConnectionInfo k) throws Exception {
    AbstractChannel channel = k.getConnectionSupplier().apply(k);
    channel.setPooled();
    return channel;
  }

  @Override
  public PooledObject<Channel> wrap(Channel v) {
    return new DefaultPooledObject<>(v);
  }

  @Override
  public void destroyObject(ConnectionInfo key, PooledObject<Channel> p) throws
          Exception {
    Channel channel = p.getObject();
    if (channel.isConnected()) {
      channel.destroy();
    }
  }

  @Override
  public boolean validateObject(ConnectionInfo key, PooledObject<Channel> p) {
    return p.getObject().isConnected() && p.getObject().isAvailable();
  }
}
