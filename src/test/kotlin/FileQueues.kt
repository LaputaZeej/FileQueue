import com.bugu.queue.FileQueue
import com.bugu.queue.transform.GsonTransform
import com.bugu.queue.transform.ProtoBuffTransform
import com.bugu.queue.transform.Transform
import com.google.protobuf.MessageLite

inline fun <reified T> createGsonFileQueue(path: String): FileQueue<T> =
    FileQueue(path, GsonTransform<T>(T::class.java))

inline fun <reified T : MessageLite> createProtoBufFileQueue(path: String): FileQueue<T> =
    FileQueue(path, ProtoBuffTransform<T>(T::class.java))

fun <T> createCustomBufFileQueue(path: String, transform: Transform<T>): FileQueue<T> =
    FileQueue(path, transform)