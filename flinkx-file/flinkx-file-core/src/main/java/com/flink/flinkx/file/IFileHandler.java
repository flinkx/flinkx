package com.flink.flinkx.file;

import java.io.InputStream;
import java.util.List;

/**
 * program: flinkx-all->IFileHandler
 * description:
 * author: gerry
 * created: 2020-05-22 09:26
 **/
public interface IFileHandler {
    /**
     * 判断给定的目录是否存在
     *
     * @param directoryPath 要检查的路径
     * @return true:存在，false:不存在
     */
    boolean isDirExist(String directoryPath);

    /**
     * 检查给定的文件是否存在
     *
     * @param filePath 要检查的文件路径
     * @return true:存在,false:不存在
     */
    boolean isFileExist(String filePath);

    /**
     * 获取文件输入流
     *
     * @param filePath 文件路径
     * @return 数据流
     */
    InputStream getInputStream(String filePath);

    /**
     * 列出指定路径下的目录
     *
     * @param path 路径
     * @return 目录列表
     */
    List<String> listDirs(String path);

    /**
     * 列出路径下的文件
     *
     * @param path 路径
     * @return 文件列表
     */
    List<String> getFiles(String path);

    /**
     * 递归创建目录
     *
     * @param directoryPath 要创建的目录
     */
    void mkDirRecursive(String directoryPath);
}
