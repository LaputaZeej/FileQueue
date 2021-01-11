package com.bugu.queue;

public class DefaultChecker implements Checker {
    @Override
    public boolean hasDisk(FileQueue fileQueue) {
        try {
            //long length = fileQueue.getLength();
            //return length <= 2 * 1024 * 1024;
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
