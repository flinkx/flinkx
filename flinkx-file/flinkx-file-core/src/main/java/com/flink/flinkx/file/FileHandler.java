package com.flink.flinkx.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        return file.isDirectory();
    }

    @Override
    public boolean isFileExist(String filePath) {
        String _path = new String(filePath.getBytes(StandardCharsets.UTF_8));
        File file = new File(_path);
        return file.isFile();
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
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(!path.endsWith(SP)) {
                path = path + SP;
            }
        }
        File file = new File(path);
        //返回目录下所有的文件以及文件夹对象
        File[] files = file.listFiles();
        if(files != null) {
            for(File ftpFile : files) {
                sources.add(path + ftpFile.getName());
            }
        }
        return sources;
    }

    @Override
    public List<String> getFiles(String path) {
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(!path.endsWith(SP)) {
                path = path + SP;
            }
            File file = new File(path);
            //返回目录下所有的文件以及文件夹对象
            File[] files = file.listFiles();
            if(files != null) {
                for(File ftpFile : files) {
                    sources.add(path + ftpFile.getName());
                }
            }
        } else if(isFileExist(path)) {
            sources.add(path);
            return sources;
        }
        return sources;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {
        StringBuilder dirPath = new StringBuilder();
        dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
        String[] dirSplit = StringUtils.split(directoryPath,IOUtils.DIR_SEPARATOR_UNIX);
        String message = String.format("创建目录:%s时发生异常,请确认file路径正常,拥有目录创建权限", directoryPath);
        try {
            // ftp server不支持递归创建目录,只能一级一级创建
            for(String dirName : dirSplit) {
                dirPath.append(dirName);
                boolean mkdirSuccess = mkDirSingleHierarchy(dirPath.toString());
                dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                if(!mkdirSuccess){
                    throw new RuntimeException(message);
                }
            }

        }catch  (Exception e) {
            message = String.format("%s, errorMessage:%s", message,
                    e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }

    }

    @Override
    public OutputStream getOutputStream(String filePath) {
        return null;
    }

    @Override
    public void deleteAllFilesInDir(String dir, List<String> exclude) {

    }

    @Override
    public void rename(String oldPath, String newPath) throws Exception {
        new File(oldPath).renameTo(new File(newPath));
    }

    /**
     * 创建文件夹
     * @param directoryPath
     * @return
     */
    private boolean mkDirSingleHierarchy(String directoryPath) {
        File file=new File(directoryPath.toString());
        // 如果directoryPath目录不存在,则创建
        if(!file.exists()) {
            return file.mkdir();//创建文件夹
        }
        return true;
    }
}
