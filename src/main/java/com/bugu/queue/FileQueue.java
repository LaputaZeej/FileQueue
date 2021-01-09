package com.bugu.queue;

import com.bugu.queue.exception.FileQueueException;
import com.bugu.queue.exception.NotEnoughDiskException;
import com.bugu.queue.transform.Transform;
import com.bugu.queue.util.RafUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final public class FileQueue<E> {


    /**
     * 默认文件长度
     */
    //    private static final long DEFAULT_LENGTH = 2 << 19; // 1M
    private static final long DEFAULT_LENGTH = 2 << 9; // 1KB

    /**
     * 记录头的位置
     */
    private static final long POINT_HEAD = 0;

    /**
     * 记录尾的位置
     */
    private static final long POINT_TAIL = 2 << 2;

    /**
     * 头文件长度
     */
    static final long HEADER_LENGTH = 2 << 3;

    private long mHeadPoint = 0;
    private long mTailPoint = 0;
    private RandomAccessFile mRaf;
    private String mPath;
    private long mLength;

    private Transform<E> transform;
    private CompressPolicy mPolicy = new DefaultCompressPolicy();
    private CapacityThreshold capacityThreshold = new DefaultCapacityThreshold();
    private Checker checker = new DefaultChecker();
    private PointerChanged pointerChanged;

    /**
     * 正在压缩空间
     */
    private AtomicBoolean mCompressing = new AtomicBoolean(false);

    private final Object mUpdateHead = new Object();
    private final Object mUpdateTail = new Object();

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * Lock held by take, poll, etc
     */
    private final ReentrantLock takeLock = new ReentrantLock();

    /**
     * Wait queue for waiting takes
     */
    private final Condition notEmpty = takeLock.newCondition();

    /**
     * Lock held by put, offer, etc
     */
    private final ReentrantLock putLock = new ReentrantLock();

    /**
     * Wait queue for waiting puts
     */
    private final Condition notFull = putLock.newCondition();

    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    public FileQueue(String path, Transform<E> transform) {
        this.mPath = path;
        this.transform = transform;
        this.mLength = DEFAULT_LENGTH;
        try {
            File file = new File(path);
            File parentFile = file.getParentFile();
            if (!parentFile.exists()) {
                boolean mkdirs = parentFile.mkdirs();
                if (!mkdirs) logger("can't create file");
            }
            if (!file.exists()) {
                boolean newFile = file.createNewFile();
                if (!newFile) logger("can't create file");
            }
            long length = file.length();
            logger("file:" + path + " , length = " + length);
            if (length >= HEADER_LENGTH) {
                RandomAccessFile r = new RandomAccessFile(file, "r");
                parseHeader(r);
            } else {
                RandomAccessFile r = new RandomAccessFile(file, "rw");
                initHeader(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initHeader(RandomAccessFile raf) {
        try {
            raf.seek(0);
            raf.writeLong(HEADER_LENGTH);
            raf.writeLong(HEADER_LENGTH);
            logger("init header complete.");
            this.mTailPoint = HEADER_LENGTH;
            this.mHeadPoint = HEADER_LENGTH;
            this.mLength = DEFAULT_LENGTH;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseHeader(RandomAccessFile raf) {

        try {
            raf.seek(0);
            long header = raf.readLong();
            long tail = raf.readLong();
            this.mHeadPoint = header;
            this.mTailPoint = tail;
            this.mLength = raf.length();
            logger("parse header. mHeadPoint = " + mHeadPoint + ", mTailPoint = " + mTailPoint + "");
            if (mHeadPoint < 0 || mTailPoint < 0 || mHeadPoint > mTailPoint) {
                logger("error file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateHead(long head) throws Exception {
        synchronized (mUpdateHead) {
            if (head > mHeadPoint) {
                RandomAccessFile r = createWriteRandomAccessFile();
                r.seek(POINT_HEAD);
                r.writeLong(head);
                mHeadPoint = head;
            }
        }
    }

    private void updateTail(long tail) throws Exception {
        synchronized (mUpdateTail) {
            if (tail > mTailPoint) {
                RandomAccessFile r = createWriteRandomAccessFile();
                r.seek(POINT_TAIL);
                r.writeLong(tail);
                mTailPoint = tail;
            }
        }
    }

    private long parseHead() throws Exception {
        RandomAccessFile r = createReadRandomAccessFile();
        r.seek(POINT_HEAD);
        return r.readLong();
    }

    private long parseTail() throws Exception {
        RandomAccessFile r = createReadRandomAccessFile();
        r.seek(POINT_TAIL);
        return r.readLong();
    }

    private void checkRandomAccessFile() throws IOException {
        if (this.mRaf == null) {
            this.mRaf = createWriteRandomAccessFile();
            this.mRaf.setLength(mLength);
        }
    }

    public RandomAccessFile createReadRandomAccessFile() throws FileNotFoundException {
        return RafUtil.createR(mPath);
    }

    private RandomAccessFile createWriteRandomAccessFile() throws FileNotFoundException {
        return RafUtil.createRW(mPath);
    }

    private void capacity() {
        logger("capacity!!!");
        mLength += DEFAULT_LENGTH;
    }

    // 剩余长度阈值
    private long vpt() {
        return capacityThreshold.capacity(this);
    }

    public void put(@NotNull E e) throws InterruptedException {
        if (mCompressing.get()) {
            System.out.println("正在清理....");
            return;
        }
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            checkRandomAccessFile();
            // todo 自动扩容
           /* if (mLength - vpt() < mTailPoint) {
                capacity();
                mRaf.setLength(mLength);
            }*/

            // 当tail到了文件末尾时，考虑扩容
            while (mTailPoint >= mLength || mLength - vpt() < mTailPoint) {
                // 扩容前，检查剩余磁盘大小
                while (!checkDiskSize()) {
                    System.out.println("< Not enough disk space ! wait... ... >");
                    // 磁盘不够 先释放文件中多余的数据（head之前的数据）
                    tryCompressDisk();
                    notFull.await();
                }
                capacity();
                mRaf.setLength(mLength);
            }

            long tail = enqueue(e);
            if (pointerChanged != null) {
                pointerChanged.onTailChanged(tail);
            }
            signalNotEmpty();
            logger("put  mTailPoint = " + tail);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
//            if (mRaf != null) {
//                mRaf.close(); // close
//            }
            putLock.unlock();
        }
    }

    public E take() throws Exception {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        long head = this.mHeadPoint;
        RandomAccessFile readRaf = null;
        try {
            while (head >= mTailPoint || mCompressing.get()) {
                System.out.println("< take nothing any more or in-compressing-disk,wait... ... >");
                notEmpty.await();
            }
            readRaf = createReadRandomAccessFile();
            E e = dequeue(readRaf);
            long filePointer = readRaf.getFilePointer();
            if (e != null /*&& mTailPoint != 0 && filePointer <= mTailPoint*/) {
                updateHead(filePointer);
//                this.mHeadPoint = readRaf.getFilePointer();
                if (pointerChanged != null) {
                    pointerChanged.onHeadChanged(this.mHeadPoint);
                }
            }
            logger("take mHeadPoint = " + this.mHeadPoint);
            return e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (readRaf != null) {
                readRaf.close();
            }
            takeLock.unlock();
        }
        throw new FileQueueException("take null");
    }

    private long enqueue(@NotNull E e) throws Exception {
        mRaf.seek(mTailPoint);
        transform.write(e, mRaf);
        long tail = mRaf.getFilePointer();
        updateTail(tail);
        return tail;
    }

    private E dequeue(RandomAccessFile readRaf) throws Exception {
        readRaf.seek(this.mHeadPoint);
        return transform.read(readRaf);
    }

    private void beforeCompressForTest() {
        for (int i = 0; i < 100; i++) {
            try {
                take();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private long compressDiskTimeOutTime = 1000;

    private void tryCompressDisk() throws NotEnoughDiskException {
        // stop all put and take first?
        executorService.submit(() -> {
            // beforeCompressForTest();
            mCompressing.set(true);
            boolean result = compressDisk();
            mCompressing.set(false);
            System.out.println("compress result = " + result);
            if (result) {
                signalNotFull();
                signalNotEmpty();
            } else {
                // throw new NotEnoughDiskException();
                // todo 停止？
                try {
                    compressDiskTimeOutTime = compressDiskTimeOutTime * 2;
                    if (compressDiskTimeOutTime > 100 * 1000) {
                        compressDiskTimeOutTime = 1000;
                    }
                    Thread.sleep(compressDiskTimeOutTime);
                    signalNotFull();
                    signalNotEmpty();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    public boolean compressDisk() {
        return mPolicy.compress(this);
    }

    /**
     * TODO  checkDiskSize
     */
    protected boolean checkDiskSize() {
        return checker.hasDisk(this);

    }

    public E remove() {
        // TODO non blocking
        return null;
    }



    @Nullable
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO non blocking
        return null;
    }

    public E peek() {
        if (mHeadPoint >= mTailPoint) {
            return null;
        }
        E e = null;
        try {
            RandomAccessFile rw = createReadRandomAccessFile();
            e = dequeue(rw);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return e;
    }

    public void close() {
        if (mRaf != null) {
            try {
                mRaf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public long getHeadPoint() {
        return mHeadPoint;
    }

    public long getTailPoint() {
        return mTailPoint;
    }

    public void setTailPoint(long mTailPoint) {
        this.mTailPoint = mTailPoint;
        if (pointerChanged != null) {
            pointerChanged.onTailChanged(mTailPoint);
        }
    }

    public long getLength() {
        return mLength;
    }

    public void setHeadPoint(long mHeadPoint) {
        this.mHeadPoint = mHeadPoint;
        if (pointerChanged != null) {
            pointerChanged.onHeadChanged(mHeadPoint);
        }
    }

    public void setLength(long mLength) {
        this.mLength = mLength;
    }

    public RandomAccessFile getWriteRaf() {
        try {
            checkRandomAccessFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mRaf;
    }


    private boolean DEBUG = true;

    private void logger(String msg) {
        if (DEBUG) System.out.println(msg);
    }

    public void setDebug(boolean debug) {
        this.DEBUG = debug;
    }
}
