package com.bugu.queue;


import java.io.IOException;
import java.io.RandomAccessFile;

public interface Transform<E> {
    void write( E e,  RandomAccessFile raf) throws Exception;

    E read( RandomAccessFile raf) throws Exception;
}
