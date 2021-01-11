package com.bugu.queue.exception;

public class NotEnoughDiskException extends Exception{
    public NotEnoughDiskException() {
    }

    public NotEnoughDiskException(String message) {
        super(message);
    }

    public NotEnoughDiskException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotEnoughDiskException(Throwable cause) {
        super(cause);
    }

    public NotEnoughDiskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
