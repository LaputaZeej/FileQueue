import com.laputa.dog._Dog
import org.junit.Test
import java.util.concurrent.TimeUnit

class FileQueueOfferUnitTest {

    private fun testFile() = createGsonFileQueue<_Dog.Dog>(
        "C:\\Users\\xpl\\Documents\\projs\\mqtt\\case\\offer_00001.txt"
    )

    @Test
    fun testClearDiskNew01() {
        println("清理磁盘")
        val fileQueue = testFile()
        fileQueue.setChecker {
            Thread.sleep(3000)
            // 测试失败
            false
        }
        println("head = ${fileQueue.headPoint}")
        println("tail = ${fileQueue.tailPoint}")
        val count = 100_000
        val dataList = createData(count)
       /* dataList.forEach {
            val offer = fileQueue.offer(it, 2000, TimeUnit.MILLISECONDS)
            if (!offer) {
                println("添加超时！")
                return@forEach
            }
        }
*/
        dataList.forEach {
            if (it.age == 10) {
                println("10!!!")
                Thread.sleep(3 *1000)
                return
            }
            println("it= $it")
        }
        fileQueue.close()
        println("清理磁盘end")
    }


}