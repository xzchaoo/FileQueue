package com.xzchaoo.filequeue.core;

/**
 * @author xiangfeng.xzc
 * @date 2019-09-28
 */
class Event {
    boolean flush;
    byte[]  data;

    Event reset() {
        flush = false;
        data = null;
        return this;
    }
}
