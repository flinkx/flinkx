package com.flink.flinkx.file;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * program: flinkx-all->FileConfig
 * description: 文件配置
 * author: gerry
 * created: 2020-05-22 09:17
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileConfig implements Serializable {

    /**
     * 文件路径
     */
    public String path;

    /**
     * 字符集
     */
    public String encoding = "UTF-8";

    /**
     * 默认分割符号
     */
    public String fieldDelimiter = FileConfigConstants.DEFAULT_FIELD_DELIMITER;

    /**
     * 第一行是否是头部
     */
    public boolean isFirstLineHeader = false;

    /**
     * 写入模式 append 、overwrite
     */
    public String writeMode;

    /**
     * 写入文件最大值
     */
    public long maxFileSize = 1024 * 1024 * 1024;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public boolean getIsFirstLineHeader() {
        return isFirstLineHeader;
    }

    public void setIsFirstLineHeader(boolean isFirstLineHeader) {
        isFirstLineHeader = isFirstLineHeader;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }
}
