package com.bugu.queue.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RafHelper {
    private static RandomAccessFile createWriteRandomAccessFile(String path, String mode)
            throws FileNotFoundException {
        return new RandomAccessFile(path, mode);
    }

    public static RandomAccessFile createR(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "r");
    }

    public static RandomAccessFile createRW(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rw");
    }

    public static RandomAccessFile createRWS(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rws");
    }

    public static RandomAccessFile createRWD(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rwd");
    }

    public static void close(RandomAccessFile r) {
        try {
            if (r != null) {
                r.close();
                 r = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
