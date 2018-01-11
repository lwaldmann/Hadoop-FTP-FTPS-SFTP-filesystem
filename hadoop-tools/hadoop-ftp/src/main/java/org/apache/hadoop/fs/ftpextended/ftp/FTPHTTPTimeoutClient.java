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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPHTTPClient;
import org.apache.hadoop.fs.ftpextended.common.ErrorStrings;

/**
 * Class needed to be able set timeout on data connection. Default
 * implementation doesn't set socket timeout leading rarely to deadlock or
 * termination of hadoop container by attempt timeout when proxy behaves
 * unexpectedly. By setting socket timeout operation is interrupted and give the
 * code possibility to handle such glitches.
 *
 */
public class FTPHTTPTimeoutClient extends FTPHTTPClient {

  private int timeout = 0;
  private final Pattern pattern = Pattern.compile("[\\[\\]]");

  public FTPHTTPTimeoutClient(String proxyHost, int proxyPort) {
    super(proxyHost, proxyPort);
  }

  public FTPHTTPTimeoutClient(String proxyHost, int proxyPort, String proxyUser,
          String proxyPass) {
    super(proxyHost, proxyPort, proxyUser, proxyPass);
  }

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
  protected Socket _openDataConnection_(String command, String arg) throws
          IOException {
   // Intercept data socket creation
    Socket socket = super._openDataConnection_(command, arg);
    if (timeout >= 0 && socket != null) {
      // And set it's timeout so we don't wait in read/write operations for ever
      socket.setSoTimeout(timeout);
    }

    return socket;
  }

  @Override
  public void setDataTimeout(int newTimeout) {
   // timeout value is private in parent class so we have to remember it
   // in extra variable
    timeout = newTimeout;
    super.setDataTimeout(timeout);
  }
}
