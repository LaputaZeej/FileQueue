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
public class DefaultCompressPolicy implements CompressPolicy {

    @Override
    public <E> boolean compress(FileQueue<E> queue) {
        display(queue, "a");
        long headPoint = queue.getHeadPoint();
        long tailPoint = queue.getTailPoint();
        long offset = tailPoint - headPoint;
        System.out.println("headPoint = " + headPoint + ",tailPoint = " + tailPoint + ",offset= " + offset);
        if (headPoint > FileQueue.HEADER_LENGTH && offset > 0) {
            long newHeadPoint = FileQueue.HEADER_LENGTH;
            long newTailPoint = FileQueue.HEADER_LENGTH + offset;
            System.out.println("newHeadPoint = " + newHeadPoint + ",newTailPoint = " + newTailPoint);
            RandomAccessFile writeRaf = queue.getWriteRaf();
            try {
                byte[] temp = new byte[1024];
                int count;
                long newHeadPointer = 0;
                long oldHeadPointer = headPoint;
                boolean writeHeaded = false;
                while (true) {
                    if (!writeHeaded) {
                        // 写入头
                        writeRaf.seek(0);
                        writeRaf.writeLong(newHeadPoint);
                        writeRaf.writeLong(newTailPoint);
                        newHeadPointer = writeRaf.getFilePointer();
                        // 写入数据 ，从旧的headPoint取出数据temp，在新的headPoint写入数据
                        writeRaf.seek(oldHeadPointer);
                        writeHeaded = true;
                    }

                    count = writeRaf.read(temp);
                    System.out.println("count:" + count);
                    if (count == -1) {
                        break;
                    }

                    //System.out.println("temp :" + new String(temp));

                    writeRaf.seek(newHeadPointer);
                    writeRaf.write(temp, 0, count);
                    oldHeadPointer += count;
                    newHeadPointer += count;
                    writeRaf.seek(oldHeadPointer);
                }
                //newLength += FileQueue.DEFAULT_LENGTH;
                writeRaf.setLength(newTailPoint);
                // 更新数据
                queue.setHeadPoint(newHeadPoint);
                queue.setTailPoint(newTailPoint);
                queue.setLength(newTailPoint);
                System.out.println("clear end.");
                //Thread.sleep(10 * 1000);
                display(queue, "b");
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
//                queue.signalNotFull();
//                queue.signalNotEmpty();
            }

        }
        // 数据不能删除，只能期望其他地方清理内存
//        queue.signalNotFull();
//        queue.signalNotEmpty();
        return false;
    }

    private static boolean debug = false;

    public static <E> void display(FileQueue<E> queue, String tag) {

        if (!debug) {
            return;
        }
        boolean isOver = false;
        try {
            RandomAccessFile rw = queue.createReadRandomAccessFile();
            long head = rw.readLong();
            long tail = rw.readLong();
            String temp;
            System.out.println("******** display start head = " + head + ",tail = " + tail + " ********");
            while (!isOver) {
                try {
                    temp = rw.readUTF();
                    System.out.println("【" + tag + "】" + temp);
                } catch (Exception e) {
                    isOver = true;
                }
            }
            System.out.println("******** display over ********");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
