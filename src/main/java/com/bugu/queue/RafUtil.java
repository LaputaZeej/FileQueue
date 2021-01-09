package com.bugu.queue;

import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class RafUtil {

    private static @NotNull
    RandomAccessFile createWriteRandomAccessFile(String path, String mode)
            throws FileNotFoundException {
        return new RandomAccessFile(path, mode);
    }

    public static @NotNull
    RandomAccessFile createR(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "r");
    }

    public static @NotNull
    RandomAccessFile createRW(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rw");
    }

    public static @NotNull
    RandomAccessFile createRWS(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rws");
    }

    public static @NotNull
    RandomAccessFile createRWD(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rwd");
    }
}
