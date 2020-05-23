package com.flink.flinkx.file.writer;

import com.flink.flinkx.config.DataTransferConfig;
import com.flink.flinkx.config.WriterConfig;
import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.util.StringUtil;
import com.flink.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flink.flinkx.file.FileConfigConstants.DEFAULT_FIELD_DELIMITER;

/**
 * program: flinkx-all->FileWriter
 * description:
 * author: gerry
 * created: 2020-05-23 07:46
 **/
public class FileWriter extends BaseDataWriter {
    private List<String> columnName;
    private List<String> columnType;
    private FileConfig fileConfig;

    public FileWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        try {
            fileConfig = objectMapper.readValue(objectMapper.writeValueAsString(writerConfig.getParameter().getAll()), FileConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析fileConfig配置出错:", e);
        }

        if(!DEFAULT_FIELD_DELIMITER.equals(fileConfig.getFieldDelimiter())){
            String fieldDelimiter = StringUtil.convertRegularExpr(fileConfig.getFieldDelimiter());
            fileConfig.setFieldDelimiter(fieldDelimiter);
        }

        List columns = writerConfig.getParameter().getColumn();
        if(columns != null && columns.size() != 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for (Object column : columns) {
                Map sm = (Map) column;
                columnName.add(String.valueOf(sm.get("name")));
                columnType.add(String.valueOf(sm.get("type")));
            }
        }
    }


    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        FileSubOutputFormatBuilder builder = new FileSubOutputFormatBuilder();
        builder.setFtpConfig(fileConfig);
        builder.setPath(fileConfig.getPath());
        builder.setMaxFileSize(fileConfig.getMaxFileSize());
        builder.setMonitorUrls(monitorUrls);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish());
    }
}
