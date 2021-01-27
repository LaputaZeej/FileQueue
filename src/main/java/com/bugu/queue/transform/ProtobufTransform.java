package com.bugu.queue.transform;

import com.bugu.queue.Transform;
import com.bugu.queue.util.Logger;
import com.google.protobuf.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ProtobufTransform<E extends MessageLite> implements Transform<E> {
    private static final String SEPARATOR = ";";
    private Parser<E> PARSER;

    public ProtobufTransform(Class<E> clz) {
        try {
            Field parser = clz.getDeclaredField("PARSER");
            parser.setAccessible(true);
            Object obj = parser.get(clz);
            PARSER = (Parser<E>) obj;
        } catch (Exception e) {
            e.printStackTrace();
            PARSER = createParse(clz);
        }
    }

    private Parser<E> createParse(Class<E> clz) {
        return new AbstractParser<E>() {
            @Override
            public E parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                try {
                    Constructor<E> constructor = clz.getDeclaredConstructor(CodedInputStream.class, ExtensionRegistryLite.class);
                    constructor.setAccessible(true);
                    E e = constructor.newInstance(input, extensionRegistry);
                    return e;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    @Override
    public void write(E e, RandomAccessFile raf) {
        try {
            byte[] bytes = e.toByteArray();
            String s = new String(bytes, StandardCharsets.ISO_8859_1);
            Logger.info("proto_write:" + s);
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
            byte[] bytes = s.getBytes(StandardCharsets.ISO_8859_1);
            E e = PARSER.parseFrom(bytes);
            Logger.info("proto_read:" + e);
            raf.readUTF();
            return e;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
