package com.flink.flinkx.file.reader;

import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.file.FileHandler;
import com.flink.flinkx.file.IFileHandler;
import com.flink.flinkx.inputformat.BaseRichInputFormat;
import com.flink.flinkx.reader.MetaColumn;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * program: flinkx-all->FileInputFormat
 * description: The InputFormat class of File
 * author: gerry
 * created: 2020-05-21 21:37
 **/
public class FileInputFormat extends BaseRichInputFormat{

    /**
     * 文件配置
     */
    protected FileConfig fileConfig;

    protected String charsetName = "utf-8";

    protected List<MetaColumn> metaColumns;

    private transient FileSeqBufferedReader br;

    private transient IFileHandler fileHandler;

    private transient String line;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        super.openInputFormat();
        fileHandler = new FileHandler();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) throws Exception {
        return new InputSplit[0];
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {
        if(br != null) {
            br.close();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        line = br.readLine();
        return line == null;
    }
}
