# File Writer

<a name="0eTYo"></a>
## 一、插件名称
名称：**filewriter**<br />

<a name="ijmKB"></a>
## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| Dir | 支持 |



<a name="2lzA4"></a>
## 三、参数说明<br />

- **path**
  - 描述：本地File文件夹系统的路径信息，注意这里可以支持填写递归单个文件夹路径
  - 必选：是
  - 默认值：无



- **fieldDelimiter**
  - 描述：读取的字段分隔符
  - 必选：是
  - 默认值：`,`



- **encoding**
  - 描述：读取文件的编码配置
  - 必选：否
  - 默认值：`UTF-8`



<br />

- **writeMode**
  - 描述：ftpwriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除dtp当前目录下的所有文件
  - 必选：否
  - 默认值：append


- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 默认值：1073741824‬（1G）



- **column**
  - 描述：需要读取的字段
  - 格式：指定具体信息：
```
"column": [{
    "name": "col1",
    "type": "datetime"
}]
```

  - 属性说明:
    - name：字段名称
    - type：字段类型，file读取的为文本文件，本质上都是字符串类型，这里可以指定要转成的类型
  - 必选：是
  - 默认值：无



<a name="lplB9"></a>
## 四、使用示例
<a name="vZyir"></a>
#### 1、append模式写入
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                              {
                                "name": "name",
                                "index": 0,
                                "type": "string"
                              },
                              {
                                "name": "age",
                                "index": 1,
                                "type": "int"
                              }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    }
                },
                "writer": {
                    "name": "ftpwriter",
                    "parameter": {
                        "path": "/Users/gerry/Desktop/www/java/flink/flinkx/dir3",
                        "writeMode": "append",
                        "column": [
                              {
                                "name": "name",
                                "index": 0,
                                "type": "string"
                              },
                              {
                                "name": "age",
                                "index": 1,
                                "type": "int"
                              }
                        ],
                        "fieldDelimiter": ",",
                        "encoding": "utf-8"
                    }
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```
<a name="JjqE3"></a>
#### 2、指定文件大小
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                              {
                                "name": "name",
                                "index": 0,
                                "type": "string"
                              },
                              {
                                "name": "age",
                                "index": 1,
                                "type": "int"
                              }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    }
                },
                "writer": {
                    "name": "ftpwriter",
                    "parameter": {
                        "path": "/Users/gerry/Desktop/www/java/flink/flinkx/dir3",
                        "writeMode": "append",
                        "column": [
                              {
                                "name": "name",
                                "index": 0,
                                "type": "string"
                              },
                              {
                                "name": "age",
                                "index": 1,
                                "type": "int"
                              }
                        ],
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "maxFileSize" : 5242880
                    }
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```


