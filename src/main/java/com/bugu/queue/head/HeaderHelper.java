package com.bugu.queue.head;

import com.bugu.queue.FileQueue;
import com.bugu.queue.Version;
import com.bugu.queue.transform.FileQueueHeadTransform;
import com.bugu.queue.util.RafHelper;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class HeaderHelper {
    public static HeaderState validateHeader(FileQueueHeader header) {
        long version = header.getVersion();
        long head = header.getHead();
        long tail = header.getTail();
        long length = header.getLength();
        if (version != Version.VERSION) {
            return HeaderState.INVALID;
        }
        if (head == 0 || tail == 0 || length == 0 ||
                head > tail ||
                tail > length
        ) {
            return HeaderState.INVALID;
        }
        if (head == tail && head == FileQueueHeader.HEADER_LENGTH) {
            return HeaderState.INIT;
        }
        if (head < tail) {
            return HeaderState.NOT_COMPLETE;
        }
        return HeaderState.COMPLETE;
    }

    public static FileQueueHeader parseHeader(String path) {
        FileQueueHeader header = null;
        RandomAccessFile raf = null;
        try {
            raf = RafHelper.createR(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            if (raf != null) {
                raf = RafHelper.createR(path);
                header = new FileQueueHeadTransform().read(raf);
                if (validateHeader(header) == HeaderState.INVALID) {
                    header = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RafHelper.close(raf);
        }
        return header;
    }
}
