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

    public String path;

    public String encoding = "UTF-8";

    public String fieldDelimiter = FileConfigConstants.DEFAULT_FIELD_DELIMITER;

    public boolean isFirstLineHeader = false;

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
}
