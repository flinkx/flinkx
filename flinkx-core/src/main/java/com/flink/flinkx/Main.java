package com.flink.flinkx;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * program: flinkx-all->Main
 * description: The main class entry
 * author: gerry
 * created: 2020-05-21 22:09
 **/
public class Main {
    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static final String READER = "reader";
    public static final String WRITER = "writer";
    public static final String STREAM_READER = "streamreader";
    public static final String STREAM_WRITER = "streamwriter";

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception{

    }
}
