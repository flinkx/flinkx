flinkx/flinkx
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# 介绍

FlinkX是一个基于Flink的批流统一的数据同步工具，既可以采集静态的数据，比如MySQL，HDFS等，也可以采集实时变化的数据，比如MySQL binlog，Kafka等。FlinkX目前包含下面这些特性：

- 大部分插件支持并发读写数据，可以大幅度提高读写速度；

- 部分插件支持失败恢复的功能，可以从失败的位置恢复任务，节约运行时间；[失败恢复](docs/restore.md)

- 关系数据库的Reader插件支持间隔轮询功能，可以持续不断的采集变化的数据；[间隔轮询](docs/offline/reader/mysqlreader.md)

- 部分数据库支持开启Kerberos安全认证；[Kerberos](docs/kerberos.md)

- 可以限制reader的读取速度，降低对业务数据库的影响；

- 可以记录writer插件写数据时产生的脏数据；

- 可以限制脏数据的最大数量；

- 支持多种运行模式；

FlinkX目前支持下面这些数据库：

|                        | Database Type  | Reader                          | Writer                          |
|:----------------------:|:--------------:|:-------------------------------:|:-------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/offline/reader/mysqlreader.md)        | [doc](docs/offline/writer/mysqlwriter.md)      |
|                        | FTP            | [doc](docs/offline/reader/ftpreader.md)          | [doc](docs/offline/writer/ftpwriter.md)        |
|                        | Stream         | [doc](docs/offline/reader/streamreader.md)       | [doc](docs/offline/writer/carbondatawriter.md) |
|                        | File            | [doc](docs/offline/reader/filereader.md)          | [doc](docs/offline/writer/filewriter.md)        |
| Stream Synchronization | Kafka          | [doc](docs/realTime/reader/kafkareader.md)       | [doc](docs/realTime/writer/kafkawriter.md)     |
|                        | MySQL Binlog   | [doc](docs/realTime/reader/binlogreader.md)      |                                                |




袋鼠云源码分析与解读，同时增加插件拓展:
- [DTStack/flinkx](https://github.com/DTStack/flinkx)