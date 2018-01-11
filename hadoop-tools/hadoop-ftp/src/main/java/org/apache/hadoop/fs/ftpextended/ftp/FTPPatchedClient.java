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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.ftpextended.common.ErrorStrings;

/**
 * FTPClient doesn't handle listing paths containing square brackets.
 */
public class FTPPatchedClient extends FTPClient {

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

}
