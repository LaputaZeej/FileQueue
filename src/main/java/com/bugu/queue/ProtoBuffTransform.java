package com.bugu.queue;

import com.google.protobuf.*;
import com.laputa.dog._Dog;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.TypeVariable;

public class ProtoBuffTransform<E extends MessageLite> implements Transform<E> {
    private static final String SEPARATOR = ";";
    private com.google.protobuf.Parser<E> PARSER; // proto会自动成成，private

    public ProtoBuffTransform(Parser<E> PARSER) {
        this.PARSER = PARSER;
    }

    public ProtoBuffTransform(Class<E> clz) {
        this.PARSER = new AbstractParser<E>() {
            @Override
            public E parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
                try {
                    Constructor<E> constructor = clz.getDeclaredConstructor(CodedInputStream.class, ExtensionRegistryLite.class);
                    constructor.setAccessible(true);
                    return constructor.newInstance(input, extensionRegistry);
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
            String s = new String(bytes);
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
            raf.readUTF();
            return PARSER.parseFrom(s.getBytes());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
