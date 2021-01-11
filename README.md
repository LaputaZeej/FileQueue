# FileQueue

一个队列，将数据序列化存储到本地文件中。

# 实现思路

使用RandomAccessFile，将数据以一定的格式序列化后，写入文件末尾，并记录末尾的指针；读的时候从文件头开始读取数据，并记录头的指针。

# 细节

### 1.一个FileQueue包含文件头和数据。
### 2.文件头（16个字节）包含头head(8个字节)和尾tail的指针(8个字节)。开始时，head和tail的值都为16
### 3.数据会以一定的格式序列化和反序列化，方便文件存入和解析。
### 4.写入数据（put）时，移动到tail，写入数据，更新tail。
### 5.读取数据（take）时，移动到head，读取数据，更新head。
### 6.目前实现文件会有一个初始的大小，当达到一定的阈值，比如初始大小的1/8时，会尝试扩容。扩容之前会先检查磁盘空间是否够用并且会尝试压缩空间，如果够用，则扩容。
### 7.压缩空间：当head>16时，说明这个文件已经读了一些,在tail之前的数据可以清理掉。

# 使用

put和take（阻塞）
```
 val fileQueue = createGsonFileQueue<Dog>(path)
 fileQueue.put(Dog("a",10))
 val dog = fileQueue.take()
```

自己实现序列化与反序列化
```
  val transform = object :Transform<Dog>{
                override fun write(data: Dog, raf: RandomAccessFile) {
                    try {
                        raf.writeUTF(data.name)
                        raf.writeInt(data.age)
                    }catch (e:Throwable){
                        e.printStackTrace()
                    }
                }

                override fun read(raf: RandomAccessFile): String {
                    try {
                        val name = raf.readUTF()
                        val age = raf.readInt()
                        return Dog(name,age)
                    }catch (e:Throwable){
                        e.printStackTrace()
                    }
                    return null
                }
            }
```
