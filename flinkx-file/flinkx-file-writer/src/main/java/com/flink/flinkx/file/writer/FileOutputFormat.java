package com.flink.flinkx.file.writer;

import com.flink.flinkx.exception.WriteRecordException;
import com.flink.flinkx.file.FileConfig;
import com.flink.flinkx.file.FileHandler;
import com.flink.flinkx.file.IFileHandler;
import com.flink.flinkx.outputformat.BaseFileOutputFormat;
import com.flink.flinkx.util.StringUtil;
import com.flink.flinkx.util.SysUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * program: flinkx-all->FileOutputFormat
 * description:
 * author: gerry
 * created: 2020-05-23 07:38
 **/
public class FileOutputFormat extends BaseFileOutputFormat {

    /**
     * 文件配置信息
     */
    protected FileConfig fileConfig;

    /**
     * 换行符
     */
    private static final int NEWLINE = 10;

    /**
     * 字段类型
     */
    protected List<String> columnTypes;

    /**
     * 字段名
     */
    protected List<String> columnNames;

    /**
     * 文件操作
     */
    private transient IFileHandler fileHandler;

    /**
     * 输出流
     */
    private transient OutputStream os;

    private static final String DOT = ".";

    /**
     * 写入文件结尾
     */
    private static final String FILE_SUFFIX = ".csv";

    /**
     * 数据写入模式 覆盖
     */
    private static final String OVERWRITE_MODE = "overwrite";

    @Override
    protected void openSource() throws IOException {
        fileHandler = new FileHandler();
    }

    @Override
    protected void checkOutputDir() {
        if(!fileHandler.isDirExist(outputFilePath)) {
            if(!makeDir){
                throw new RuntimeException("Output path not exists:" + outputFilePath);
            }
            fileHandler.mkDirRecursive(outputFilePath);
        }else {
            if(OVERWRITE_MODE.equalsIgnoreCase(fileConfig.getWriteMode()) && !SP.equals(outputFilePath)){
                fileHandler.deleteAllFilesInDir(outputFilePath, null);
                fileHandler.mkDirRecursive(outputFilePath);
            }
        }
    }

    @Override
    protected void cleanDirtyData() {
        int fileIndex = formatState.getFileIndex();
        String lastJobId = formatState.getJobId();
        LOG.info("fileIndex = {}, lastJobId = {}",fileIndex, lastJobId);
        if(org.apache.commons.lang3.StringUtils.isBlank(lastJobId)){
            return;
        }
        List<String> files = fileHandler.getFiles(outputFilePath);
        files.removeIf(new Predicate<String>() {
            @Override
            public boolean test(String file) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if(!fileName.contains(lastJobId)){
                    return true;
                }

                String[] splits = fileName.split("\\.");
                if (splits.length == 3) {
                    return Integer.parseInt(splits[2]) <= fileIndex;
                }

                return true;
            }
        });

        if(CollectionUtils.isNotEmpty(files)){
            for (String file : files) {
                fileHandler.deleteAllFilesInDir(file, null);
            }
        }
    }

    @Override
    protected void nextBlock(){
        super.nextBlock();

        if (os != null){
            return;
        }

        os = fileHandler.getOutputStream(tmpPath + SP + currentBlockFileName);
        blockIndex++;
    }

    @Override
    protected void moveTemporaryDataBlockFileToDirectory() {
        if (currentBlockFileName == null || !currentBlockFileName.startsWith(DOT)){
            return;
        }

        try{
            String src = fileConfig.getPath() + SP + tmpPath + SP + currentBlockFileName;
            if (!fileHandler.isFileExist(src)) {
                LOG.warn("block file {} not exists", src);
                return;
            }

            currentBlockFileName = currentBlockFileName.replaceFirst("\\.", StringUtils.EMPTY);
            String dist = fileConfig.getPath() + SP + tmpPath + SP + currentBlockFileName;
            fileHandler.rename(src, dist);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void writeSingleRecordToFile(Row row) throws WriteRecordException {
        if(os == null){
            nextBlock();
        }

        String line = StringUtil.row2string(row, columnTypes, fileConfig.getFieldDelimiter());
        try {
            byte[] bytes = line.getBytes(fileConfig.getEncoding());
            this.os.write(bytes);
            this.os.write(NEWLINE);
            this.os.flush();

            if(restoreConfig.isRestore()){
                lastRow = row;
                rowsOfCurrentBlock++;
            }
        } catch(Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }


    @Override
    protected void createFinishedTag() throws IOException {
        LOG.info("Subtask [{}] finished, create dir {}", taskNumber, finishedPath);
        fileHandler.mkDirRecursive(finishedPath);
    }

    @Override
    protected boolean isTaskEndsNormally(){
        try{
            String state = getTaskState();
            if(!RUNNING_STATE.equals(state)){
                if (!restoreConfig.isRestore()){
                    fileHandler.deleteAllFilesInDir(tmpPath, null);
                }

                return false;
            }
        } catch (IOException e){
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    protected void createActionFinishedTag() {
        fileHandler.mkDirRecursive(actionFinishedTag);
    }

    @Override
    protected void waitForActionFinishedBeforeWrite() {
        boolean readyWrite = fileHandler.isDirExist(actionFinishedTag);
        int n = 0;
        while (!readyWrite){
            if(n > SECOND_WAIT){
                throw new RuntimeException("Wait action finished before write timeout");
            }

            SysUtil.sleep(1000);
            LOG.info("action finished tag path:{}", actionFinishedTag);
            readyWrite = fileHandler.isDirExist(actionFinishedTag);
            n++;
        }
    }


    @Override
    protected void waitForAllTasksToFinish() throws IOException {
        final int maxRetryTime = 100;
        int i = 0;
        for (; i < maxRetryTime; i++) {
            int finishedTaskNum = fileHandler.listDirs(outputFilePath + SP + FINISHED_SUBDIR).size();
            LOG.info("The number of finished task is:{}", finishedTaskNum);
            if(finishedTaskNum == numTasks){
                break;
            }

            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
            fileHandler.deleteAllFilesInDir(finishedPath, null);
            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }
    }

    @Override
    protected void coverageData() throws IOException {
        boolean cleanPath = restoreConfig.isRestore() && OVERWRITE_MODE.equalsIgnoreCase(fileConfig.getWriteMode()) && !SP.equals(fileConfig.getPath());
        if(cleanPath){
            fileHandler.deleteAllFilesInDir(fileConfig.getPath(), Arrays.asList(tmpPath));
        }
    }


    @Override
    protected void moveTemporaryDataFileToDirectory() throws IOException {
        try{
            List<String> files = fileHandler.getFiles(tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && fileName.startsWith(String.valueOf(taskNumber))){
                    String newPath = fileConfig.getPath() + SP + fileName;
                    LOG.info("Move file {} to path {}", file, newPath);
                    fileHandler.rename(file, newPath);
                }
            }
        }catch (Exception e){
            throw new RuntimeException("Rename temp file error:", e);
        }
    }

    @Override
    protected void moveAllTemporaryDataFileToDirectory() throws IOException {
        try{
            List<String> files = fileHandler.getFiles(tmpPath);
            for (String file : files) {
                String fileName = file.substring(file.lastIndexOf(SP) + 1);
                if (fileName.endsWith(FILE_SUFFIX) && !fileName.startsWith(DOT)){
                    String newPath = fileConfig.getPath() + SP + fileName;
                    LOG.info("Move file {} to path {}", file, newPath);
                    fileHandler.rename(file, newPath);
                }
            }
        }catch (Exception e){
            throw new RuntimeException("Rename temp file error:", e);
        }
    }

    @Override
    protected void closeSource() throws IOException {
        if (os != null){
            os.flush();
            os.close();
            os = null;
        }
    }

    @Override
    protected void clearTemporaryDataFiles() throws IOException {
        fileHandler.deleteAllFilesInDir(tmpPath, null);
        LOG.info("Delete .data dir:{}", tmpPath);

        fileHandler.deleteAllFilesInDir(outputFilePath + SP + FINISHED_SUBDIR, null);
        LOG.info("Delete .finished dir:{}", outputFilePath + SP + FINISHED_SUBDIR);
    }


    @Override
    protected void flushDataInternal() throws IOException {
        closeSource();
    }

    @Override
    public float getDeviation() {
        return 0;
    }

    @Override
    protected String getExtension() {
        return ".csv";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("FileWriter");
    }
}
