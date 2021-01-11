import com.laputa.dog._Dog
import org.junit.Test

fun startThread(block: () -> Unit) {
    Thread(block).start()
}

fun _Dog.Dog.println() {
    println("dog[$name,$age]")
}

fun createData(count: Int): List<_Dog.Dog> {
    return (1..count).map {
        _Dog.Dog.newBuilder().setAge(it).setName("【1】$it").build()
    }
}

class Demo {
    @Test
    fun t1() {
        println("start")
        kotlin.run {
            (1..20).forEach {
                println("index= $it")
                if (it == 10) {
                    println("is 10 !")
                    return@run
                }
            }
        }
        println("end")
    }
}




