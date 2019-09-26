package com.xzchaoo.filequeue.core;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xzchaoo
 */
public class SpscFileQueueTest {
    @Test
    public void test() throws IOException {
        File file = Files.createTempDirectory("SpscFileQueueTest").toFile();
        System.out.println(file.exists());
        SpscFileQueue q = new SpscFileQueue(file);
        q.enqueue(new byte[] {1, 2, 3});
        assertThat(q.hasData()).isTrue();
        assertThat(q.dequeue()).containsExactly(1, 2, 3);
        assertThat(q.peek()).isNull();

        q.enqueue(new byte[] {1, 2, 3});
        assertThat(q.peek()).containsExactly(1, 2, 3);
        assertThat(q.peek()).containsExactly(1, 2, 3);
        assertThat(q.skip()).isTrue();
        assertThat(q.skip()).isFalse();

        q.close();

    }

    @Test
    public void test2() throws IOException, InterruptedException {
        File file = Files.createTempDirectory("SpscFileQueueTest").toFile();
        System.out.println(file.exists());
        SpscFileQueue q = new SpscFileQueue(file);
        int count = 1111;
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                q.enqueue(new byte[]{1});
            }
            System.out.println("enqueue done");
        });
        Thread t2 = new Thread(() -> {
            int c = 0;
            while (true) {
                byte[] e = q.dequeue();
                if (e != null) {
                    if (++c == count) {
                        break;
                    }
                }
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        q.close();

    }
}
