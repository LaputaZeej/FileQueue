package com.bugu.queue;

import com.bugu.queue.head.FileQueueHeader;
import com.bugu.queue.head.HeaderHelper;
import com.bugu.queue.head.HeaderState;
import com.bugu.queue.util.Logger;
import com.bugu.queue.util.Size;

import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.bugu.queue.ImmutableFileQueue.MIN_SIZE;

public class ClipFileQueue<E> implements FileQueue<E> {
    private ImmutableFileQueue<E> fileQueue;
    private OnFileQueueChanged onFileQueueChanged;
    private long max;
    private static final long MAX_SIZE = Size._G;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public ClipFileQueue(String path, Transform<E> transform) {
        this(path, MIN_SIZE, MAX_SIZE, transform);
    }

    public ClipFileQueue(String path, long max, Transform<E> transform) {
        this(path, MIN_SIZE, max, transform);
    }

    long capacity;
    Transform<E> transform;
    String path;


    public ClipFileQueue(String path, long capacity, long max, Transform<E> transform) {
        this.max = max;
        this.path = path;
        this.capacity = capacity;
        this.transform = transform;
        initFileList();
    }

    private void initFileList() {
        FileQueueHeader header = HeaderHelper.parseHeader(path);
        if (header == null) {
            fileQueue = new ImmutableFileQueue<E>(path, capacity, transform) {
                @Override
                public boolean checkDiskFull() {
                    return getHeader().getLength() >= max;
                }
            };
        } else {
            long extra = header.getExtra();// 0xffffffff

        }
    }

    private final ReentrantLock clippingLock = new ReentrantLock();
    private final Condition notClip = clippingLock.newCondition();

    private void tryClipping(FileQueueHeader fileQueueHeader) {
        RandomAccessFile writeRaf = null;
        final ReentrantLock lock = this.clippingLock;
        try {
            long tail = fileQueueHeader.getTail();
            long length = fileQueueHeader.getLength();
            if (Math.abs(length - tail) < MIN_SIZE / 8) {
                length = length + MIN_SIZE;

                lock.lockInterruptibly();
                while (length > max) {
                    Logger.info("tryClipping!!!");
                    doClip();
                    clippingLock.wait();
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private int index = 0;


    private void doClip() {
        FileQueueHeader header = getHeader();
        long extra = header.getExtra();

        executorService.execute(() -> {
            // clip
            index++;
            String path = fileQueue.getPath();
            signalNotClipping();
        });
    }

    private void signalNotClipping() {
        final ReentrantLock takeLock = this.clippingLock;
        takeLock.lock();
        try {
            notClip.signal();
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public void close() {
        fileQueue.close();
    }

    @Override
    public void put(E e) throws Exception {
        fileQueue.put(e);
    }

    @Override
    public E take() throws Exception {
        return fileQueue.take();
    }

    @Override
    public FileQueueHeader getHeader() {
        return fileQueue.getHeader();
    }

    public void setOnFileQueueChanged(OnFileQueueChanged onFileQueueChanged) {
        this.onFileQueueChanged = onFileQueueChanged;
    }
}
