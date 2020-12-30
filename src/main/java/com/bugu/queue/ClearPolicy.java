package com.bugu.queue;

public interface ClearPolicy {
    <E> boolean clear(FileQueue<E> queue);
}
