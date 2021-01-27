package com.bugu.queue;

import com.bugu.queue.head.FileQueueHeader;
import com.bugu.queue.util.Logger;
import com.bugu.queue.util.Size;

import java.io.RandomAccessFile;

import static com.bugu.queue.ImmutableFileQueue.MIN_SIZE;

public class MutableFileQueue<E> implements FileQueue<E> {
    private ImmutableFileQueue<E> fileQueue;
    private OnFileQueueChanged onFileQueueChanged;
    private long max;
    private static final long MAX_SIZE = Size._G;

    public MutableFileQueue(ImmutableFileQueue<E> fileQueue, long max) {
        this.max = max;
        initFileQueue(fileQueue);
    }

    public MutableFileQueue(String path, Transform<E> transform) {
        this(path, MIN_SIZE, MAX_SIZE, transform);
    }

    public MutableFileQueue(String path, long max, Transform<E> transform) {
        this(path, MIN_SIZE, max, transform);
    }


    public MutableFileQueue(String path, long capacity, long max, Transform<E> transform) {
        this.max = max;
        this.fileQueue = new ImmutableFileQueue<E>(path, capacity, transform);
        initFileQueue(fileQueue);
    }

    private void initFileQueue(ImmutableFileQueue<E> fileQueue) {
        this.fileQueue = fileQueue;
        this.fileQueue.setCheckDiskCallback((fq) -> fq.getHeader().getLength() >= MutableFileQueue.this.max);
        this.fileQueue.setOnFileQueueChanged((fq, type, header) -> {
            if (type == 0) {
                tryCapacity(header);
            }
            if (onFileQueueChanged != null) {
                onFileQueueChanged.onChanged(fq, type, header);
            }
        });
    }

    private void tryCapacity(FileQueueHeader fileQueueHeader) {
        RandomAccessFile writeRaf = null;
        try {
            long tail = fileQueueHeader.getTail();
            long length = fileQueueHeader.getLength();
            if (Math.abs(length - tail) < MIN_SIZE / 8) {
                length = length + MIN_SIZE;
                if (length > max) {
                    return;
                }
                Logger.info("capacity!!!");
                fileQueueHeader.setLength(length);
                writeRaf = fileQueue.getHeaderRaf();
                fileQueue.getHeader().setLength(length);
                fileQueue.getLengthPoint().write(writeRaf, length);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

    @Override
    public String getPath() {
        return fileQueue.getPath();
    }

    @Override
    public boolean delete() {
        return fileQueue.delete();
    }

    public void setOnFileQueueChanged(OnFileQueueChanged onFileQueueChanged) {
        this.onFileQueueChanged = onFileQueueChanged;
    }
}
