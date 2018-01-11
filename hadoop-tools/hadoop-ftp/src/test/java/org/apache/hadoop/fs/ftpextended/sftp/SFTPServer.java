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
package org.apache.hadoop.fs.ftpextended.sftp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.hadoop.fs.ftpextended.common.Server;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.io.IoSession;
import org.apache.sshd.common.io.IoWriteFuture;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.UserAuthNoneFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.session.ServerSessionImpl;
import org.apache.sshd.server.session.SessionFactory;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;

/**
 * SFTP test server.
 */
public class SFTPServer implements Server {
  private static SshServer sshd = null;
  private static int port;

  public SFTPServer(String root) throws IOException {
    sshd = SshServer.setUpDefaultServer();
    // ask OS to assign a port
    sshd.setPort(0);
    sshd.setSessionFactory(new SessionFactory(sshd) {
      @Override
      protected ServerSessionImpl doCreateSession(IoSession ioSession) throws
              Exception {
        return new ServerSessionImpl(this.getServer(), ioSession) {
          @Override
          protected IoWriteFuture sendIdentification(String ident) {
            try {
             // wait a bit so connection is established before sending ident
             // Seems to be a bug in littleproxy
              Thread.sleep(50); // NOSONAR
            } catch (InterruptedException ex) {
            }
            return super.sendIdentification(ident);
          }
        };
      }
    });
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

    File rootDir = new File(root);
    rootDir.mkdir();
    sshd.setFileSystemFactory(new VirtualFileSystemFactory(
            rootDir.toPath()));
    List<NamedFactory<UserAuth>> userAuthFactories
            = new ArrayList<>();
    userAuthFactories.add(new UserAuthNoneFactory());

    sshd.setUserAuthFactories(userAuthFactories);

    sshd.setPasswordAuthenticator((String username,
            String password,
            ServerSession session) ->
            username.equals("user") && password.equals("password")
    );
    sshd.setSubsystemFactories(
            Arrays.<NamedFactory<Command>>asList(new SftpSubsystemFactory()));

    sshd.start();
    port = sshd.getPort();
  }

  @Override
  public void stop() {
    if (sshd != null) {
      try {
        sshd.stop();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public FtpServerFactory getServerFactory() {
    throw new UnsupportedOperationException("Not supported for this server.");
  }
}
