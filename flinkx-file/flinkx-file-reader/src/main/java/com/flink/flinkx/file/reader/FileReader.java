package com.flink.flinkx.file.reader;

import com.flink.flinkx.config.DataTransferConfig;
import com.flink.flinkx.config.ReaderConfig;
import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.reader.BaseDataReader;
import com.flink.flinkx.reader.MetaColumn;
import com.flink.flinkx.util.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

import static com.flink.flinkx.file.FileConfigConstants.DEFAULT_FIELD_DELIMITER;

/**
 * program: flinkx-all->FileReader
 * description:
 * author: gerry
 * created: 2020-05-22 12:06
 **/
public class FileReader extends BaseDataReader {

    private List<MetaColumn> metaColumns;
    private FileConfig fileConfig;


    public FileReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            fileConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), FileConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析fileConfig配置出错:", e);
        }

        if(!DEFAULT_FIELD_DELIMITER.equals(fileConfig.getFieldDelimiter())){
            String fieldDelimiter = StringUtil.convertRegularExpr(fileConfig.getFieldDelimiter());
            fileConfig.setFieldDelimiter(fieldDelimiter);
        }

        List columns = readerConfig.getParameter().getColumn();
        metaColumns = MetaColumn.getMetaColumns(columns, false);
    }

    @Override
    public DataStream<Row> readData() {
        FileInputFormatBuilder builder = new FileInputFormatBuilder();
        builder.setFileConfig(fileConfig);
        builder.setMetaColumn(metaColumns);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }
}
