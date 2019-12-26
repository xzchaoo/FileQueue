package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xzchaoo
 */
public class SpscFileQueueTest {
    @Test
    public void test() throws IOException {
        File dir = Files.createTempDirectory("SpscFileQueueTest").toFile();
        SpscFileQueueSpec spec = new SpscFileQueueSpec();
        spec.setDir(dir);
        FileQueue q = FileQueues.spsc(spec);
        q.enqueue(new byte[] {1, 2, 3});
        assertThat(q.hasData()).isTrue();
        assertThat(q.dequeue()).isEqualTo(new byte[] {1, 2, 3});
        assertThat(q.peek()).isNull();

        q.enqueue(new byte[] {1, 2, 3});
        assertThat(q.peek()).isEqualTo(new byte[] {1, 2, 3});
        assertThat(q.peek()).isEqualTo(new byte[] {1, 2, 3});
        assertThat(q.skip()).isTrue();
        assertThat(q.skip()).isFalse();
        q.enqueue(new byte[] {1, 2, 3});
        q.dequeue();
        q.enqueue(new byte[] {1, 2, 3});
        q.dequeue();
        q.close();
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        // File file = Files.createTempDirectory("SpscFileQueueTest").toFile();
        File dir = new File("ignore_me", "SpscFileQueueTest/test2");
        SpscFileQueue q = (SpscFileQueue) FileQueues.spsc(dir);
        q.log();
        int count = 1000000;
        int count2 = count / 2;
        Thread t1 = new Thread(() -> {
            byte[] bytes = "手动阀时间点看立法健身房考虑到计量看是交流反馈爱神的箭福克斯了封疆大吏".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < count; i++) {
                q.enqueue(bytes);
                // if (i == count2) {
                //     System.exit(1);
                // }
            }
            System.out.println("enqueue done");
        });
        Thread t2 = new Thread(() -> {
            int c = 0;
            while (true) {
                byte[] e = q.dequeue();
                if (e != null) {
                    if (++c == count) {
                        System.out.println(q.hasData());
                        break;
                    }
                }
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        System.out.println(q.hasData());
        q.close();
    }
}
