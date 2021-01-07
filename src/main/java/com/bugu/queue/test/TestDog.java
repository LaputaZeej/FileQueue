//package com.bugu.queue.test;
//
//import java.lang.reflect.Constructor;
//
//public class TestDog {
//    public static Dog parse(){
//        try {
//            Constructor<Dog> constructor = Dog.class.getDeclaredConstructor(String.class, int.class,int.class);
//            constructor.setAccessible(true);
//            return constructor.newInstance("haha",1,2);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return null;
//    }
//}
