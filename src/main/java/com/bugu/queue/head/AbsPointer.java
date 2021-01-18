package com.bugu.queue.head;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class AbsPointer implements Pointer {
    @Override
    public void write(RandomAccessFile raf, long value) throws IOException {
        raf.seek(point());
        raf.writeLong(value);
    }

    @Override
    public long read(RandomAccessFile raf) throws IOException {
        raf.seek(point());
        return raf.readLong();
    }
}
