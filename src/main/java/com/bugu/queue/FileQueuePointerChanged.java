package com.bugu.queue;

public interface FileQueuePointerChanged {
    void onHeadChanged(long head);

    void onTailChanged(long tail);
}
