package com.xzchaoo.filequeue.core;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.cert.TrustAnchor;
import java.util.stream.StreamSupport;

/**
 * @author xzchaoo
 * @date 2019/12/19
 */
public class FileAppendPerformanceTest {
    @Test
    public void test_jdk7_files() throws IOException {
        Path path = Paths.get("E:\\temp\\FileAppendPerformanceTest");
        Files.deleteIfExists(path);
        BufferedWriter bw = Files.newBufferedWriter(path, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE_NEW);
        doWithBw(bw);
    }

    private void doWithBw(BufferedWriter bw) throws IOException {
        boolean flush = true;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            bw.write("test " + i);
            bw.newLine();
            if (flush) {
                bw.flush();
            }
        }
        bw.flush();
        bw.close();
        long end = System.currentTimeMillis();
        System.out.println(end - begin);
    }

    @Test
    public void test() throws IOException {
        File file = new File("E:\\temp\\FileAppendPerformanceTest");
        file.delete();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
        doWithBw(bw);
    }
}
