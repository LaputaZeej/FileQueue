package com.bugu.queue.transform;

import com.laputa.dog._Dog;

import java.io.IOException;
import java.io.RandomAccessFile;

// 测试类 单个的测试
public class ProtoBuffTransformTestDog implements Transform<_Dog.Dog> {
    private static final String SEPARATOR = ";";

    @Override
    public void write(_Dog.Dog e, RandomAccessFile raf) {
        try {
            byte[] bytes = e.toByteArray();
            String s = new String(bytes);
            raf.writeUTF(s);
            raf.writeUTF(SEPARATOR);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public _Dog.Dog read(RandomAccessFile raf) {
        try {
            String s = raf.readUTF();
            raf.readUTF();
            return _Dog.Dog.parseFrom(s.getBytes());
//            byte[] temp = new byte[1024];
//            int count;
//            String s = raf.readUTF();
//            while (-1 != (count = raf.read(temp))) {
//                System.out.println("count:" + count);
//            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
