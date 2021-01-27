package com.bugu.queue;

import com.bugu.queue.exception.FileQueueException;
import com.bugu.queue.head.FileQueueHeader;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface FileQueue<E> {

    void close();

//    boolean add(E e);
//
//    boolean offer(E e);

    void put( E e) throws Exception;

//    boolean offer(E e, long timeout, TimeUnit unit)
//            throws Exception;

    E take() throws Exception;

    FileQueueHeader getHeader() ;

    String getPath();

    boolean delete() ;
//
//    E peek();
//
//    E poll(long timeout, TimeUnit unit)
//            throws Exception;
//
//    int remainingCapacity();
//
//    boolean remove(Object o);
//
//    public boolean contains(Object o);
//
//    int drainTo(Collection<? super E> c);
//
//    int drainTo(Collection<? super E> c, int maxElements);
}
