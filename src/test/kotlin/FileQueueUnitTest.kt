import com.laputa.dog._Dog
import org.junit.Test

class FileQueueUnitTest {

    private fun testClearDiskNewFile() = createGsonFileQueue<_Dog.Dog>(
        "C:\\Users\\xpl\\Documents\\projs\\mqtt\\case\\case_clearDisk_00011.txt"
    )

    @Test
    fun testClearDiskNew01() {
        println("清理磁盘")
        val fileQueue = testClearDiskNewFile()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")

        println("开始放入数据")
        val count = 1000
        val dataList = createData(count)
        dataList.forEach {
            fileQueue.put(it)
        }
        repeat(count / 2) {
            val take = fileQueue.take()
            take.println()
        }
        fileQueue.close()
        println("清理磁盘end")
    }

    @Test
    fun testClearDiskNew02() {
        println("清理磁盘")
        val fileQueue = testClearDiskNewFile()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")
        fileQueue.compressDisk()
        Thread.sleep(1000)
        println("清理磁盘end")
    }

    @Test
    fun testClearDiskNew03() {
        println("清理磁盘")
        val fileQueue = testClearDiskNewFile()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")
        while (true) {
            val take = fileQueue.take()
            take.println()
        }
        fileQueue.close()
        println("清理磁盘end")
    }


    private fun testClearDiskInFileQueue() = createGsonFileQueue<_Dog.Dog>(
        "C:\\Users\\xpl\\Documents\\projs\\mqtt\\case\\case_clearDisk_in_file_queue_0013.txt"
    )

    @Test
    fun testClearDiskInFileQueue01() {
        println("testClearDiskInFileQueue01")
        val fileQueue = testClearDiskInFileQueue()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")

        println("开始放入数据")
        val count = 100 * 1000
        val dataList = createData(count).asSequence()
        dataList.forEach {
            fileQueue.put(it)
        }
//        repeat(count / 2) {
//            val take = fileQueue.take()
//            take.println()
//        }
        fileQueue.close()
        println("testClearDiskInFileQueue01 end ")
    }

    @Test
    fun testClearDiskInFileQueue02() {
        println("清理磁盘")
        val fileQueue = testClearDiskInFileQueue()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")
        while (true) {
            val take = fileQueue.take()
            take.println()
        }
        fileQueue.close()
        println("清理磁盘end")
    }

    private fun testClearDisk() = createGsonFileQueue<_Dog.Dog>(
        "C:\\Users\\xpl\\Documents\\projs\\mqtt\\case\\case_clearDisk_0001.txt"
    )

    @Test
    fun testClearDisk01() {
        println("清理磁盘")
        val fileQueue = testClearDisk()
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")
        while (true) {
            val take = fileQueue.take()
            take.println()
        }
        fileQueue.close()
        println("清理磁盘end")
    }
}