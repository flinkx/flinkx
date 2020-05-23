package com.flink.flinkx.file.reader;

import com.flink.flinkx.constants.ConstantValue;
import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.file.FileHandler;
import com.flink.flinkx.file.IFileHandler;
import com.flink.flinkx.inputformat.BaseRichInputFormat;
import com.flink.flinkx.reader.MetaColumn;
import com.flink.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * 字符集
     */
    protected String charsetName = "utf-8";

    /**
     * json定义column数据格式
     */
    protected List<MetaColumn> metaColumns;

    /**
     * 文件流读取
     */
    private transient FileSeqBufferedReader br;

    /**
     * 文件操作
     */
    private transient IFileHandler fileHandler;

    /**
     * 读取行数
     */
    private transient String line;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        fileHandler = new FileHandler();
    }


    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        IFileHandler fileHandler = new FileHandler();
        List<String> files = new ArrayList<>();
        String path = fileConfig.getPath();
        if(path != null && path.length() > 0){
            path = path.replace("\n","").replace("\r","");
            String[] pathArray = StringUtils.split(path, ",");
            for (String p : pathArray) {
                files.addAll(fileHandler.getFiles(p.trim()));
            }
        }
        int numSplits = (Math.min(files.size(), minNumSplits));
        FileInputSplit[] fileInputSplits = new FileInputSplit[numSplits];
        for(int index = 0; index < numSplits; ++index) {
            fileInputSplits[index] = new FileInputSplit();
        }
        for(int i = 0; i < files.size(); ++i) {
            fileInputSplits[i % numSplits].getPaths().add(files.get(i));
        }

        return fileInputSplits;
    }

    @Override
    protected void openInternal(InputSplit split) throws IOException {
        FileInputSplit inputSplit = (FileInputSplit)split;
        List<String> paths = inputSplit.getPaths();

        if (fileConfig.getIsFirstLineHeader()){
            br = new FileSeqBufferedReader(fileHandler,paths.iterator());
            br.setFromLine(1);
        } else {
            br = new FileSeqBufferedReader(fileHandler,paths.iterator());
            br.setFromLine(0);
        }
        br.setCharsetName(charsetName);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        String[] fields = line.split(fileConfig.getFieldDelimiter());
        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(fields.length);
            for (int i = 0; i < fields.length; i++) {
                row.setField(i, fields[i]);
            }
        }else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object value = null;
                if(metaColumn.getIndex() != null && metaColumn.getIndex() < fields.length){
                    value = fields[metaColumn.getIndex()];
                    if(((String) value).length() == 0){
                        value = metaColumn.getValue();
                    }
                } else if(metaColumn.getValue() != null){
                    value = metaColumn.getValue();
                }

                if(value != null){
                    value = StringUtil.string2col(String.valueOf(value),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i, value);
            }
        }

        return row;
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
