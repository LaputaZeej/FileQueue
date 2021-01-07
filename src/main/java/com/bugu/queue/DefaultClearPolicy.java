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
                // д��ͷ
                writeRaf.seek(0);
                writeRaf.writeLong(newHeadPoint);
                writeRaf.writeLong(newTailPoint);
                long newLength = writeRaf.getFilePointer(); // 16
                long pointTemp = headPoint;
                // д������ ���Ӿɵ�headPointȡ������temp�����µ�headPointд������
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
                // ��������
                queue.setHeadPoint(newHeadPoint);
                queue.setTailPoint(newTailPoint);
                queue.setLength(newLength);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // ���ݲ���ɾ����ֻ�����������ط������ڴ�
        return false;
    }
}
