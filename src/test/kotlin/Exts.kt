import com.laputa.dog._Dog

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




