package com.xzchaoo.filequeue.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author xiangfeng.xzc
 * @date 2019-09-28
 */
public class MpscFileQueueImpl extends SpscFileQueue implements MpscFileQueue {

    private Disruptor<Event> disruptor;

    public MpscFileQueueImpl(File dir) throws IOException {
        super(dir);
        init();
    }

    public MpscFileQueueImpl(File dir, long fileSize, int maxAllowedDataFileCount) throws IOException {
        super(dir, fileSize, maxAllowedDataFileCount);
        init();
    }

    private void init() {
        disruptor = new Disruptor<>(
                Event::new,
                1024,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return null;
                    }
                },
                ProducerType.MULTI,
                new LiteTimeoutBlockingWaitStrategy(1, TimeUnit.SECONDS)
        );
        disruptor.handleEventsWith((EventHandler<Event>) (event, sequence, endOfBatch) -> {
            if (event.flush) {
                super.flush();
            } else {
                super.enqueue(event.data);
            }
        });
        // disruptor.setDefaultExceptionHandler(null);
        disruptor.start();
    }

    @Override
    public void enqueue(byte[] data, boolean wait) {
        // TODO wait
        disruptor.publishEvent(new EventTranslatorOneArg<Event, byte[]>() {
            @Override
            public void translateTo(Event event, long sequence, byte[] arg0) {
                event.reset();
                event.data = arg0;
            }
        }, data);
    }

    @Override
    public void flush(boolean wait) {
        disruptor.publishEvent(new EventTranslator<Event>() {
            @Override
            public void translateTo(Event event, long sequence) {
                event.reset().flush = true;
            }
        });
    }
}
