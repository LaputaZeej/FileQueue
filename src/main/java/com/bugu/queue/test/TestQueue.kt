package com.bugu.queue.test

import com.bugu.queue.FileQueue
import com.bugu.queue.Transform
import java.io.IOException
import java.io.RandomAccessFile

fun main() {
    testQ()
}

fun testQ() {
    val path = "C:\\Users\\xpl\\Documents\\projs\\mqtt\\hhhhhh.txt"
    val fileQueue = FileQueue<Dog>(path, object : Transform<Dog> {
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

    println("head = ${fileQueue.headPoint}")
    println("tail = ${fileQueue.tailPoint}")

    /*  println("准备开始清理多余数据")
      fileQueue.clearDisk()*/

    // put
    startThread {
        (1..10).map {
            Dog("[1]dog$it", it)
        }.forEach {
            Thread.sleep(10)
            fileQueue.put(it)
        }
    }

    // take
    startThread {
        Thread.sleep(1000)
        val take = fileQueue.take()
        println("[1]取头：$take")
    }

    // put
    startThread {
        Thread.sleep(20 * 1000)
        (1..10000).map {
            Dog("[2]dog$it", it)
        }.forEach {
            Thread.sleep(10)
            fileQueue.put(it)
        }
    }

    // take
    startThread {
        while (true) {
            Thread.sleep(1000)
            val take = fileQueue.take()
            println("[1]取头：$take")
        }
    }
}

private fun startThread(block: () -> Unit) {
    Thread(block).start()
}