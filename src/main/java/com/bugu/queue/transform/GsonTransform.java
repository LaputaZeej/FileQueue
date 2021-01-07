package com.bugu.queue.transform;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * json 协议存数据
 * @param <E>
 */
public  class GsonTransform<E> implements Transform<E> {
    private Gson gson;
    private Class<E> clz;
    private static final String SEPARATOR = ";";

    public GsonTransform(Class<E> clz) {
        gson = new Gson();
        this.clz = clz;
    }

    @Override
    public void write(E e, RandomAccessFile raf) {
        try {
            String json = gson.toJson(e);
            raf.writeUTF(json);
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
