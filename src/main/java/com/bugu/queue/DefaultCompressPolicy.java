package com.bugu.queue;

import java.io.RandomAccessFile;

/**
 * ��headPoint֮ǰ�������������ʡ�ڴ�
 * ������
 * 1.�ȼ����µ�headPoint��tailPoint
 * 2.��0д���µ�ͷ
 * 3.�Ӿɵ�headPoint����������
 * 4.�Ѷ��������ݴ��µ�headPointд�룬�������µ�tailPoint
 * 5.�ظ�3&4��ֱ��û���µ�����
 * 6.�����ļ����ȣ�����head��tail���ݡ�
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
                        // д��ͷ
                        writeRaf.seek(0);
                        writeRaf.writeLong(newHeadPoint);
                        writeRaf.writeLong(newTailPoint);
                        newHeadPointer = writeRaf.getFilePointer();
                        // д������ ���Ӿɵ�headPointȡ������temp�����µ�headPointд������
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
                // ��������
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
        // ���ݲ���ɾ����ֻ�����������ط������ڴ�
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
                    System.out.println("��" + tag + "��" + temp);
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
