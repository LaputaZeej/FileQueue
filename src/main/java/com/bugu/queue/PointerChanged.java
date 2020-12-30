package com.bugu.queue;

public interface PointerChanged {
    void onHeadChanged(long head);

    void onTailChanged(long tail);
}
