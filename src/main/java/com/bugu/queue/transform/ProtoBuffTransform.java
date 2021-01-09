package com.bugu.queue.transform;

import com.google.protobuf.*;
import sun.nio.cs.UTF_8;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ProtoBuffTransform<E extends MessageLite> implements Transform<E> {
    private static final String SEPARATOR = ";";
    private com.google.protobuf.Parser<E> PARSER; // proto会自动成成，private

    // PARSER是私有的
//    public ProtoBuffTransform(Parser<E> PARSER) {
//        this.PARSER = PARSER;
//    }

    // 反射静态对象
    public ProtoBuffTransform(Class<?> clz) {
        try {
            Field parser = clz.getDeclaredField("PARSER");
            parser.setAccessible(true);
            Object obj = parser.get(clz);
            PARSER = (com.google.protobuf.Parser<E>)obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

      // 自己创建的无效
//    public ProtoBuffTransform(Class<E> clz) {
//        this.PARSER = new AbstractParser<E>() {
//            @Override
//            public E parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
//                try {
//                    Constructor<E> constructor = clz.getDeclaredConstructor(CodedInputStream.class, ExtensionRegistryLite.class);
//                    constructor.setAccessible(true);
//                    E e = constructor.newInstance(input, extensionRegistry);
//                    return constructor.newInstance(input, extensionRegistry);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return null;
//            }
//        };
//    }


    @Override
    public void write(E e, RandomAccessFile raf) {
        try {
            byte[] bytes = e.toByteArray();
            String s = new String(bytes,StandardCharsets.UTF_8);
            raf.writeUTF(s);
            raf.writeUTF(SEPARATOR);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public E read(RandomAccessFile raf) {
        try {
            String s = raf.readUTF();
            E e = PARSER.parseFrom(s.getBytes(StandardCharsets.UTF_8));
            raf.readUTF();
            return e;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
