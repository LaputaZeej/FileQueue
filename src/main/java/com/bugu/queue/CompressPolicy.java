package com.bugu.queue;

/**
 * 压缩空间
 */
public interface CompressPolicy {
    <E> boolean compress(FileQueue<E> queue);
}
