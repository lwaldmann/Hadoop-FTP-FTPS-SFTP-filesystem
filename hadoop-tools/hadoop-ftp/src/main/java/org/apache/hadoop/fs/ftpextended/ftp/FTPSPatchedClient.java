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
package org.apache.hadoop.fs.ftpextended.ftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLSocket;
import org.apache.commons.net.ftp.FTPCommand;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.hadoop.fs.ftpextended.common.ErrorStrings;

/**
 * FTPSClient doesn't handle listing paths containing square brackets.
 * Hadoop uses commons.net version 3.1 which has incorrect implementation of
 * ftps client. This subclass fixes the issue - won't be necessary if we moved
 * to version 3.6 of commons.net
 */
public class FTPSPatchedClient extends FTPSClient {

  private final Pattern pattern = Pattern.compile("[\\[\\]]");

  @Override
  public FTPFile[] listFiles(String pathname) throws IOException {
    if (pathname == null) {
      return super.listFiles(null);
    }
    Matcher matcher = pattern.matcher(pathname);
    if (matcher.find()) {
      String wd = printWorkingDirectory();
      if (changeWorkingDirectory(pathname)) {
        FTPFile[] ftpFiles = super.listFiles(null);
        changeWorkingDirectory(wd);
        return ftpFiles;
      } else {
        throw new FileNotFoundException(String.format(
                ErrorStrings.E_SPATH_NOTEXIST, pathname));
      }
    } else {
      return super.listFiles(pathname);
    }
  }

  @Override
  protected Socket _openDataConnection_(String command, String arg)
      throws IOException {
    Socket socket = super._openDataConnection_(command, arg);
    _prepareDataSocket_(socket);
    if (socket instanceof SSLSocket) {
      SSLSocket sslSocket = (SSLSocket)socket;

      sslSocket.setUseClientMode(getUseClientMode());
      sslSocket.setEnableSessionCreation(getEnableSessionCreation());

      // server mode
      if (!getUseClientMode()) {
        sslSocket.setNeedClientAuth(getNeedClientAuth());
        sslSocket.setWantClientAuth(getWantClientAuth());
      }
      if (getEnabledCipherSuites() != null) {
        sslSocket.setEnabledCipherSuites(getEnabledCipherSuites());
      }
      if (getEnabledProtocols() != null) {
        sslSocket.setEnabledProtocols(getEnabledProtocols());
      }
      sslSocket.startHandshake();
    }

    return socket;

  }

  @Override
  protected Socket _openDataConnection_(int command, String arg)
          throws IOException {
    return _openDataConnection_(FTPCommand.getCommand(command), arg);
  }

}
