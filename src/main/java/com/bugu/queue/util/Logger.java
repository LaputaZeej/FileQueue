package com.bugu.queue.util;

public class Logger {

    private static final String TAG = "[FileQueue] ";
    private static boolean DEBUG = true;

    public static void setDebug(boolean debug) {
        DEBUG = debug;
    }

    public static void logger(String msg) {
        if (DEBUG) System.out.println(TAG + msg);
    }


}
