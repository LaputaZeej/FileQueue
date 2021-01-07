package com.bugu.queue.transform;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 测试类 用byte存
 */
public  class GsonTransformTestJson<E> implements Transform<E> {
    private Gson gson;
    private Class<E> clz;
    private static final String SEPARATOR = ";";

    public GsonTransformTestJson(Class<E> clz) {
        gson = new Gson();
        this.clz = clz;
    }

    @Override
    public void write(E e, RandomAccessFile raf) {
        try {
            String json = gson.toJson(e);
            byte[] bytes = json.getBytes();
            raf.write(bytes);
            raf.writeUTF(SEPARATOR);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public E read(RandomAccessFile randomAccessFile) {
        try {
            String json = randomAccessFile.readUTF();
            randomAccessFile.readUTF();
            E e = gson.fromJson(json, clz);
            return e;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
