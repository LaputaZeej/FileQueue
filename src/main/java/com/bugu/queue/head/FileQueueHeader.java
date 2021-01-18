package com.bugu.queue.head;

import com.bugu.queue.util.Size;

public class FileQueueHeader {

    public static final long HEADER_LENGTH = 40/*Long.SIZE * 5 >> 3*/;
//    public static final long VERSION = 0;
//    public static final long HEAD = 8;
//    public static final long TAIL = 16;
//    public static final long LENGTH = 24;
//    public static final long extra = 32;

    public FileQueueHeader() {
    }

    public FileQueueHeader(long version, long head, long tail, long length, long extra) {
        this.version = version;
        this.head = head;
        this.tail = tail;
        this.length = length;
        this.extra = extra;
    }

    private long version;
    private long head;
    private long tail;
    private long length;
    private long extra;

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getHead() {
        return head;
    }

    public void setHead(long head) {
        this.head = head;
    }

    public long getTail() {
        return tail;
    }

    public void setTail(long tail) {
        this.tail = tail;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getExtra() {
        return extra;
    }

    public void setExtra(long extra) {
        this.extra = extra;
    }

    @Override
    public String toString() {
        return "FileQueueHeader{" +
                "version=" + version +
                ", head=" + head +
                ", tail=" + tail +
                ", length=" + length +
                ", extra=" + extra +
                ", length=" + length/ Size._M +
                '}';
    }
}
