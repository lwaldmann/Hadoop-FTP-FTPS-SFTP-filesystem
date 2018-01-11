<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. See accompanying LICENSE file.
-->

Hadoop ftp/ftps/sftp filesystem
======================

Backport, rewrite and improvements of the current ftp filesystem and of hadoop sftp filesystem from Hadoop 2.8

New features:
-----
* Support for HTTP/SOCKS proxies
* Support for passive FTP
* Support for explicit FTPS
* Support of connection pooling - new connection is not created for every single command but reused from the pool.
For huge number of files it shows order of magnitude performance improvement over not pooled connections.
* Caching of directory trees. For ftp you always need to list whole directory whenever you ask information about particular file.
Again for huge number of files it shows order of magnitude performance improvement over not cached connections.
* Support of keep alive (NOOP) messages to avoid connection drops
* Support for Unix style or regexp wildcard glob - useful for listing particular files across whole directory tree
* Support for reestablishing broken ftp data transfers - can happen surprisingly often

Schemas and theirs implementing classes
-------
FTP: org.apache.hadoop.fs.ftpextended.ftp.FTPFileSystem
FTPS: org.apache.hadoop.fs.ftpextended.ftp.FTPFileSystem
SFTP: org.apache.hadoop.fs.ftpextended.sftp.SFTPFileSystem

Sample usage
-----
```
hadoop distcp
            -Dfs.<schema>.password.<hostname>.<user>=<passwd>
            -Dfs.<schema>.impl=<FQN of implementing class>
            <schema>://<user>@<hostname>/<src_file> <dst_file>

hdfs dfs
    -Dfs.<schema>.impl=<FQN of implementing class> -ls  <schema>://<user>@<hostname>/
```



Properties
-----
* fs.\<schema\>.connection.max - max number of parallel connections stored in connection pool
    * values: int
    * default: 5
* fs.\<schema\>.host - server host name
* fs.\<schema\>.host.port - server port
    * values: int
    * default: FTP 21, SFTP 22
* fs.\<schema\>.user.\<hostname\> - user name used connecting to \<hostname\>
    * values: string
    * default: anonymous
* fs.\<schema\>.password.\<hostname\>.\<user\> - password used when connecting as \<user\> to \<hostname\>
    * values: string
    * default: anonymous@domain.com
* fs.\<schema\>.proxy.type - type of proxy
    * values: enum - NONE/HTTP/SOCKS4/SOCKS5
    * default: NONE
* fs.\<schema\>.proxy.host - proxy host name
* fs.\<schema\>.proxy.port - proxy port
    * values: int
    * default: HTTP 8080, SOCKS 1080
* fs.\<schema\>.proxy.user - proxy user name
* fs.\<schema\>.proxy.password - proxy user password
* fs.\<schema\>.glob.type - how to process search queries (wildcard processing)
    * values: enum - UNIX/REGEXP
    * default: UNIX
* fs.\<schema\>.cache.\<hostname\> - cache accessed files and directories for particular hostname, can significantly improves performance for big file trees
    * values: boolean
    * default: false
* fs.\<schema\>.use.keepalive - prevent connection cancelation by sending NOOP commands
    * values: boolean
    * default: false
* fs.\<schema\>.use.keepalive.period - period for sending NOOP in minutes
    * values: int
    * default: 1

Supported proxies
-----------------
FTP: HTTP
FTPS: NONE
SFTP: HTTP, SOCKS4, SOCKS5

Contract integration test
-------------------------
Contract integration tests will by default use built in ftp/sftp test server. If you would like to use different servers please modify src/test/resources/contract/params.xml file so properties fs.contract.use.internal.ftpserver resp. fs.contract.use.internal.sftpserver are set to false and specify the proper endpoints in src/test/resources/contract-test-options.xml


TODO
----
key file for sftp  - easy for local use but for distcp - distributing to the nodes is problematic

SOCKS support for ftp

add support for appendFile

add support for CWD

