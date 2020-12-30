package com.bugu.queue;

import java.io.RandomAccessFile;

/**
 * 转换器
 * 如何将数据转成字节码，以及如何解析字节码为数据
 */
public interface Transform<E> {
    void write(E e, RandomAccessFile raf);

    E read(RandomAccessFile randomAccessFile);
}
