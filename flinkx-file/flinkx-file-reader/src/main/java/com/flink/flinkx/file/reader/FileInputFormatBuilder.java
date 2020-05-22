package com.flink.flinkx.file.reader;

import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.flink.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * program: flinkx-all->FileInputFormatBuilder
 * description:
 * author: gerry
 * created: 2020-05-21 21:39
 **/
public class FileInputFormatBuilder extends BaseRichInputFormatBuilder{

    /**
     * 输入文件格式
     */
    private FileInputFormat format;

    /**
     * 构造文件输入格式
     */
    public FileInputFormatBuilder() {
        super.format = format = new FileInputFormat();
    }

    /**
     * 设置配置文件
     * @param fileConfig
     */
    public void setFileConfig(FileConfig fileConfig){
        format.fileConfig = fileConfig;
    }


    public void setMetaColumn(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }


    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }

        if (StringUtils.isEmpty(format.fileConfig.getPath())) {
            throw new IllegalArgumentException("The property [path] cannot be empty or null");
        }
    }
}
