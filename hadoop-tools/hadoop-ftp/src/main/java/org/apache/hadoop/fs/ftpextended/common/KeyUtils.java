/*
 * Copyright 2018 Apache Software Foundation.
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
package org.apache.hadoop.fs.ftpextended.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.bouncycastle.openssl.PEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to obtain transparently passwords and keys.
 */
public final class KeyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KeyUtils.class);

  private KeyUtils() {};

  static byte[] getKey(ConnectionInfo info, String path) {
    byte[] key = null;
    if (path != null) {
      key = getKeyFromFile(info, path);
    }
    if (key == null) {
      return getKeyFromKeystore(info);
    } else {
      return key;
    }
  }

  static char[] getKeyPassphrase(ConnectionInfo info) {
    try {
      return info.getConf().getPassword(info.getFtpHost() + "_" +
              info.getFtpUser() + "_key_passphrase");
    } catch (IOException ex) {
      return null;
    }
  }

  private static byte[] getKeyFromFile(ConnectionInfo info, String path) {
    try {
      URI keyURI = new URI(path);
      FileSystem fs = FileSystem.get(keyURI, info.getConf());
      try (FSDataInputStream dis = fs.open(new Path(keyURI))) {
        byte[] buffer = new byte[dis.available()];
        dis.readFully(0, buffer);
        return buffer;
      }
    } catch (IOException | URISyntaxException ex) {
      LOG.error(info.logWithInfo("Key can't be obtained"), ex);
    }
    return null;
  }

  /**
   * Return Private key associated with the ftp user.
   * Key store key used is user_key
   */
  private static byte[] getKeyFromKeystore(ConnectionInfo info) {
    try {
      List<CredentialProvider> providers
              = CredentialProviderFactory.getProviders(info.getConf());
      for (CredentialProvider provider : providers) {
        if (provider instanceof AbstractJavaKeyStoreProvider) {
          AbstractJavaKeyStoreProvider jks
                  = (AbstractJavaKeyStoreProvider) provider;
          KeyStore ks = jks.getKeyStore();
          Key key = ks.getKey(info.getFtpHost() + "_" + info.getFtpUser() +
                  "_key", getKeystorePassword(info));
          if (key != null) {
            StringWriter stringWriter = new StringWriter();
            try (PEMWriter pemWriter = new PEMWriter(stringWriter)) {
              pemWriter.writeObject(key);
            }
            return stringWriter.toString().getBytes(StandardCharsets.UTF_8);
          }
        }
      }
    } catch (IOException | KeyStoreException | NoSuchAlgorithmException |
            UnrecoverableKeyException ex) {
      LOG.error(info.logWithInfo("Key can't be obtained"), ex);
    }
    return null;
  }

  /**
   *  Returns password associated with the key store.
   */
  private static char[] getKeystorePassword(ConnectionInfo info) throws
          IOException {
    char[] password
        = locatePassword(
              AbstractJavaKeyStoreProvider.CREDENTIAL_PASSWORD_NAME,
        info.getConf()
              .get(AbstractJavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY));
    if (password == null) {
      password
        = AbstractJavaKeyStoreProvider.KEYSTORE_PASSWORD_DEFAULT
                .toCharArray();
    }
    return password;
  }

  /**
   * The password is either found in the environment or in a file. This
   * routine implements the logic for locating the password in these
   * locations.
   *
   * @param envWithPass  The name of the environment variable that might
   *                     contain the password. Must not be null.
   * @param fileWithPass The name of a file that could contain the password.
   *                     Can be null.
   * @return The password as a char []; null if not found.
   * @throws IOException If fileWithPass is non-null and points to a
   * nonexistent file or a file that fails to open and be read properly.
   */
  private static char[] locatePassword(String envWithPass, String fileWithPass)
      throws IOException {
    char[] pass = null;
    if (System.getenv().containsKey(envWithPass)) {
      pass = System.getenv(envWithPass).toCharArray();
    }
    if (pass == null) {
      if (fileWithPass != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL pwdFile = cl.getResource(fileWithPass);
        if (pwdFile == null) {
          // Provided Password file does not exist
          throw new IOException("Password file does not exist");
        }
        try (InputStream is = pwdFile.openStream()) {
          pass = IOUtils.toString(is).trim().toCharArray();
        }
      }
    }
    return pass;
  }
}
