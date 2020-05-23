package com.flink.flinkx.file.writer;

import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.outputformat.FileOutputFormatBuilder;

import java.util.List;

/**
 * program: flinkx-all->FileOutputFormatBuilder
 * description:
 * author: gerry
 * created: 2020-05-23 07:39
 **/
public class FileSubOutputFormatBuilder extends FileOutputFormatBuilder {
    private FileOutputFormat format;

    public FileSubOutputFormatBuilder() {
        format = new FileOutputFormat();
        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setFtpConfig(FileConfig fileConfig){
        format.fileConfig = fileConfig;
    }

    @Override
    protected void checkFormat() {

    }

}
