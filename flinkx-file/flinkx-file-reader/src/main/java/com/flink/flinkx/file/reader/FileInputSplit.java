package com.flink.flinkx.file.reader;

import org.apache.flink.core.io.InputSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * program: flinkx-all->FtpInputSplit
 * description:
 * author: gerry
 * created: 2020-05-22 12:05
 **/
public class FileInputSplit implements InputSplit {
    private List<String> paths = new ArrayList<>();

    @Override
    public int getSplitNumber() {
        return 0;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }
}
