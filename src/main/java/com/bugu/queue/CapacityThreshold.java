package com.bugu.queue;

/**
 * 扩容阈值
 */
public interface CapacityThreshold {
    long capacity(FileQueue fileQueue);
}
