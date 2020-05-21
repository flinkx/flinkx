package com.flink.flinkx.file.reader;

import com.flink.flinkx.inputformat.BaseRichInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * program: flinkx-all->FileInputFormat
 * description: The InputFormat class of File
 * author: gerry
 * created: 2020-05-21 21:37
 **/
public class FileInputFormat extends BaseRichInputFormat{
    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

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

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }
}
