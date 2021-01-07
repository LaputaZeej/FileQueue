package com.bugu.queue;

import java.io.RandomAccessFile;

/**
 * 把headPoint之前的数据清除，节省内存
 * 做法：
 * 1.先计算新的headPoint和tailPoint
 * 2.从0写入新的头
 * 3.从旧的headPoint处读出数据
 * 4.把读出的数据从新的headPoint写入，并更新新的tailPoint
 * 5.重复3&4，直到没有新的数据
 * 6.更新文件长度，更新head和tail数据。
 */
public class DefaultClearPolicy implements ClearPolicy {

    @Override
    public <E> boolean clear(FileQueue<E> queue) {
        long headPoint = queue.getHeadPoint();
        long tailPoint = queue.getTailPoint();
        long r = tailPoint - headPoint;
        System.out.println("r = " + r);
        if (headPoint > FileQueue.HEADER_LENGTH && r > 0) {
            long newHeadPoint = FileQueue.HEADER_LENGTH;
            long newTailPoint = FileQueue.HEADER_LENGTH + r;
            System.out.println("newHeadPoint = " + newHeadPoint);
            System.out.println("newTailPoint = " + newTailPoint);
            RandomAccessFile writeRaf = queue.getWriteRaf();
            try {
                System.out.println("old length = " + writeRaf.length());
                // 写入头
                writeRaf.seek(0);
                writeRaf.writeLong(newHeadPoint);
                writeRaf.writeLong(newTailPoint);
                long newLength = writeRaf.getFilePointer(); // 16
                long pointTemp = headPoint;
                // 写入数据 ，从旧的headPoint取出数据temp，在新的headPoint写入数据
                writeRaf.seek(pointTemp);
                byte[] temp = new byte[1024];
                int count;
                while (-1 != (count = writeRaf.read(temp))) {
                    System.out.println("count:" + count);
                    writeRaf.seek(newLength);
                    writeRaf.write(temp, 0, count);
                    pointTemp += count;
                    newLength += count;
                    writeRaf.seek(pointTemp);
                }
                //newLength += FileQueue.DEFAULT_LENGTH;
                writeRaf.setLength(newLength);
                System.out.println("newLength = " + newLength);
                System.out.println("clear end.");
                // 更新数据
                queue.setHeadPoint(newHeadPoint);
                queue.setTailPoint(newTailPoint);
                queue.setLength(newLength);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 数据不能删除，只能期望其他地方清理内存
        return false;
    }
}
