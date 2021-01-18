package com.bugu.queue;

import com.bugu.queue.head.FileQueueHeader;

public interface OnFileQueueChanged {
    void onChanged(FileQueue<?> fileQueue, int type,FileQueueHeader header);
}
