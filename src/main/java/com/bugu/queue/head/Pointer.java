package com.bugu.queue.head;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface Pointer {
    long point();

    void write(RandomAccessFile raf, long value) throws IOException;

    long read(RandomAccessFile raf) throws IOException;
}
