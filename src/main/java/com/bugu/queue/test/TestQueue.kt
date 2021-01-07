package com.bugu.queue.test

import com.bugu.queue.*
import com.bugu.queue.transform.ProtoBuffTransformTestDog
import com.bugu.queue.transform.GsonTransform
import com.bugu.queue.transform.ProtoBuffTransform
import com.bugu.queue.transform.Transform
import com.laputa.dog._Dog
import java.io.IOException
import java.io.RandomAccessFile

fun main() {
//    testPutNormal()
//    testPutGson()
//    testPutProto()
//    testPeekProto()
//    testTakeProto()
    testPutProtoParse()

}

fun testPeekProto() {

    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_proto_02.txt"
    val fileQueue = protoFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    val peek = fileQueue.peek()
    println("peek ${peek?.name} - ${peek?.age}")
}

fun testTake() {
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_gson_01.txt"
    val fileQueue = gsonFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // take
    startThread {
        while (true) {
            Thread.sleep(1000)
            val take = fileQueue.take()
            println("[1]取头：$take")
        }
    }
}

fun testTakeProto() {
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_proto_03.txt"
    val fileQueue = protoFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // take
    startThread {
        while (true) {
            Thread.sleep(1000)
            val take = fileQueue.take()
            println("[1]取头：$take")
        }
    }
}

fun testPutGson() {
    // capacity 80 81kb
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_gson_01.txt"
    val fileQueue = gsonFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // put
    startThread {
        createData().forEach {
            fileQueue.put(it)
        }
    }
}

fun testPutNormal() {
    // capacity 34 35kb
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_normal_01.txt"
    val fileQueue = normalFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // put
    startThread {
        createData().forEach {
            fileQueue.put(it)
        }
    }
}


fun testPutProto() {
    // capacity 39 39kb
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_proto_03.txt"
    val fileQueue = protoFileQueue(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // put
    startThread {
        createData().map {
            _Dog.Dog.newBuilder().setAge(it.age).setName(it.name).build()
        }.forEach {
            fileQueue.put(it)
        }
    }
}

fun testPutProtoParse() {
    // capacity
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\put_proto_06_parse.txt"
    val fileQueue = protoFileQueueParse(path)
    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")
    // put
//    startThread {
//        createData().map {
//            _Dog.Dog.newBuilder().setAge(it.age).setName(it.name).build()
//        }.forEach {
//            fileQueue.put(it)
//        }
//    }

    // take
    startThread {
        while (true) {
            Thread.sleep(1000)
            val take = fileQueue.take()
            println("[1]取头：( $take )")
        }
    }
}

private fun createData(): List<Dog> {
    return (1..2000).map {
        Dog("[2]dog$it", it)
    }
}

private fun protoFileQueueParse(path: String): FileQueue<_Dog.Dog> {
//    val fileQueue = FileQueue<_Dog.Dog>(path, ProtoBuffTransform<_Dog.Dog>(_Dog.Dog.PARSER)) // PARSER私有的
    val fileQueue = FileQueue<_Dog.Dog>(
        path, ProtoBuffTransform<_Dog.Dog>(_Dog.Dog::class.java)
    )
    return fileQueue
}

private fun protoFileQueue(path: String): FileQueue<_Dog.Dog> {
    val fileQueue = FileQueue<_Dog.Dog>(path, ProtoBuffTransformTestDog())
    return fileQueue
}

private fun gsonFileQueue(path: String): FileQueue<Dog> {
    val fileQueue = FileQueue<Dog>(path, GsonTransform<Dog>(Dog::class.java))
    return fileQueue
}

private fun normalFileQueue(path: String): FileQueue<Dog> {
    return FileQueue<Dog>(path, object : Transform<Dog> {
        override fun write(dog: Dog, raf: RandomAccessFile) {
            try {
                raf.writeUTF(dog.name)
                raf.writeInt(dog.age)
            } catch (e: IOException) {
                e.printStackTrace()
            }
        }

        override fun read(raf: RandomAccessFile): Dog? {
            var dog: Dog? = null
            try {
                dog = Dog()
                dog.name = raf.readUTF()
                dog.age = raf.readInt()
            } catch (e: IOException) {
                e.printStackTrace()
            }
            return dog
        }
    })
}

private fun startThread(block: () -> Unit) {
    Thread(block).start()
}