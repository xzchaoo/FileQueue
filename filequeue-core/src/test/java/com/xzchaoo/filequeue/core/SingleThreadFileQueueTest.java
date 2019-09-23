package com.xzchaoo.filequeue.core;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xzchaoo
 */
public class SingleThreadFileQueueTest {
    @Test
    public void test() throws IOException {
        File file = Files.createTempDirectory("SingleThreadFileQueueTest").toFile();
        System.out.println(file.exists());
        SingleThreadFileQueue q = new SingleThreadFileQueue(file);
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
}
