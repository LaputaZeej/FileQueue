package com.bugu.queue.transform;

import java.io.RandomAccessFile;

/**
 * ת����
 * ��ν�����ת���ֽ��룬�Լ���ν����ֽ���Ϊ����
 */
public interface Transform<E> {
    void write(E e, RandomAccessFile raf);

    E read(RandomAccessFile randomAccessFile);
}
