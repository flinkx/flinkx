package com.flink.flinkx.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * program: flinkx-all->FileHandler
 * description:
 * author: gerry
 * created: 2020-05-22 09:27
 **/
public class FileHandler implements IFileHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FileHandler.class);

    private static final String SP = "/";

    @Override
    public boolean isDirExist(String directoryPath) {
        String dirPath = new String(directoryPath.getBytes(StandardCharsets.UTF_8));
        File file = new File(dirPath);
        return file.exists();
    }

    @Override
    public boolean isFileExist(String filePath) {
        String _path = new String(filePath.getBytes(StandardCharsets.UTF_8));
        File file = new File(_path);
        return file.exists();
    }

    @Override
    public InputStream getInputStream(String filePath) {
        String _path = new String(filePath.getBytes(StandardCharsets.UTF_8));
        try {
            //实例化InputStream类对象
            return new FileInputStream(_path);
        } catch (FileNotFoundException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public List<String> listDirs(String path) {
        return null;
    }

    @Override
    public List<String> getFiles(String path) {
        return null;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {

    }
}
