# File Reader

<a name="lq07M"></a>
## 一、插件名称
名称：**filereader**<br />

<a name="AsTTs"></a>
## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| File | 支持 |
| Dir | 支持 |



<a name="2lzA4"></a>
## 三、参数说明<br />

- **path**
  - 描述：本地File/Dir文件系统的路径信息，注意这里可以支持填写多个路径
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



- **isFirstLineHeader**
  - 描述：首行是否为标题行，如果是则不读取第一行
  - 必选：否
  - 默认值：false
  


- **column**
  - 描述：需要读取的字段
  - 格式：支持2中格式
<br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：
```
"column":["*"]
```

2.指定具体信息：
```
"column": [{
    "index": 0,
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

  - 属性说明:
    - index：字段索引
    - type：字段类型，file读取的为文本文件，本质上都是字符串类型，这里可以指定要转成的类型
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果没有指定index，则会把value的值作为常量列返回，如果指定了index，当读取的字段的值为null时，会以此value值作为默认值返回
  - 必选：是
  - 默认值：无



<a name="QQaDC"></a>
## 四、使用示例
<a name="1nZ3r"></a>
#### 1、读取单个文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "filereader",
                    "parameter": {
                        "path": "/Users/gerry/Desktop/www/java/flink/flinkx/dir/file1.txt",
                        "isFirstLineHeader": true,
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
                        "fieldDelimiter": ":",
                        "encoding": "utf-8"
                     }
                },
                "writer": {
                    "parameter": {},
                    "name": "filewriter"
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
<a name="BTnag"></a>
#### 2、读取单个目录下的所有文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "filereader",
                    "parameter": {
                        "path": "/Users/gerry/Desktop/www/java/flink/flinkx/dir",
                        "isFirstLineHeader": true,
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
                        "fieldDelimiter": ":",
                        "encoding": "utf-8"
                     }
                },
                "writer": {
                    "parameter": {},
                    "name": "filewriter"
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
<a name="KxOTY"></a>
#### 3、读取多个路径下的文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "filereader",
                    "parameter": {
                        "path": "/Users/gerry/Desktop/www/java/flink/flinkx/dir1,/Users/gerry/Desktop/www/java/flink/flinkx/dir2",
                        "isFirstLineHeader": true,
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
                },
                "writer": {
                    "parameter": {},
                    "name": "filewriter"
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


