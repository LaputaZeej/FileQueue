package com.bugu.queue.transform;

import com.bugu.queue.Transform;
import com.google.gson.Gson;

import java.io.RandomAccessFile;

public class GsonTransform<E> implements Transform<E> {
    private Gson gson;
    private Class<E> clz;

    public GsonTransform(Class<E> clz) {
        this.gson = new Gson();
        this.clz = clz;
    }

    @Override
    public void write(E e, RandomAccessFile raf) throws Exception {
        try {
            String json = gson .toJson(e);
            raf.writeUTF(json);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public E read(RandomAccessFile raf) throws Exception {
        try {
            String json = raf.readUTF();
            return gson.fromJson(json,clz);
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
}
