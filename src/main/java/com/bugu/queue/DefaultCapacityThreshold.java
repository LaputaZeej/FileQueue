package com.bugu.queue;

public class DefaultCapacityThreshold implements CapacityThreshold {
    @Override
    public  long capacity(FileQueue fileQueue) {
        return fileQueue.getLength() >> 3;
    }
}
