package com.bugu.queue.transform;

import com.bugu.queue.Transform;
import com.bugu.queue.head.FileQueueHeader;
import com.bugu.queue.head.HeadPointer;
import com.bugu.queue.util.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileQueueHeadTransform implements Transform<FileQueueHeader> {

    @Override
    public void write(FileQueueHeader fileQueueHeader, RandomAccessFile raf) throws IOException {
        try {
            Logger.info("write Head : " + fileQueueHeader.toString());
            raf.writeLong(fileQueueHeader.getVersion());
            raf.writeLong(fileQueueHeader.getHead());
            raf.writeLong(fileQueueHeader.getTail());
            raf.writeLong(fileQueueHeader.getLength());
            raf.writeLong(fileQueueHeader.getExtra());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public FileQueueHeader read(RandomAccessFile raf) throws IOException {
        try {
            FileQueueHeader header = new FileQueueHeader();
            header.setVersion(raf.readLong());
            header.setHead(raf.readLong());
            header.setTail(raf.readLong());
            header.setLength(raf.readLong());
            header.setExtra(raf.readLong());
            Logger.info("readHead : " + header.toString());
            return header;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
