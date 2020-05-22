package com.flink.flinkx.file.reader;

import com.flink.flinkx.file.FileHandler;
import com.flink.flinkx.file.IFileHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * program: flinkx-all->FileSeqBufferedReader
 * description:
 * author: gerry
 * created: 2020-05-22 09:48
 **/
public class FileSeqBufferedReader {

    private IFileHandler fileHandler;

    private InputStream in;

    private Iterator<String> iter;

    private int fromLine = 0;

    private BufferedReader br;

    private String charsetName = "utf-8";


    public FileSeqBufferedReader(IFileHandler fileHandler, Iterator<String> iter) {
        this.fileHandler = fileHandler;
        this.iter = iter;
    }

    public String readLine() throws IOException {
        if (br == null){
            nextStream();
        }

        if(br != null){
            String line = br.readLine();
            if (line == null){
                close();
                return readLine();
            }

            return line;
        } else {
            return null;
        }
    }

    private void nextStream() throws IOException{
        if(iter.hasNext()){
            String file = iter.next();
            in = fileHandler.getInputStream(file);
            if (in == null) {
                throw new NullPointerException();
            }

            br = new BufferedReader(new InputStreamReader(in, charsetName));

            for (int i = 0; i < fromLine; i++) {
                br.readLine();
            }
        } else {
            br = null;
        }
    }

    public void close() throws IOException {
        if (br != null){
            br.close();
            br = null;

            if (fileHandler instanceof FileHandler){
                //最后记得，关闭流
                in.close();
            }
        }
    }

    public void setFromLine(int fromLine) {
        this.fromLine = fromLine;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

}
