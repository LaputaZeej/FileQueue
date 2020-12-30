package com.bugu.queue;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 【文件队列】todo
 * <p>
 * 以文件为数据载体，实现了读take写put各自阻塞队列。
 * 文件以一定的格式存储，前16字节存储了头mHeadPoint和尾mTailPoint的指针位置
 * 初始值头和尾都为0，文件大小默认值
 * 添加数据时，更新尾指针位置，并写入文件头。
 * 获取数据时，更新头指针位置，并写入文件头。
 * <p>
 * 当尾指针快超过文件大小时，扩容
 * 当物理内存达到一定阈值时，尝试清理头指针之前的数据，并更新文件长度。
 * <p>
 * <p>
 * 【文件格式】
 * --文件头16字节
 * ----head 0xFFFFFFFF
 * ----tail 0xFFFFFFFF
 * --数据
 * ----读自定义
 * ----取自定义
 */
final public class FileQueue<E> implements PointerChanged {

    private static final long DEFAULT_LENGTH = 2 * 1024 * 1024;
    private static final long POINT_HEAD = 0;
    private static final long POINT_TAIL = 2 << 2;
    static final long HEADER_LENGTH = 2 << 3;

    private long mHeadPoint = 0;
    private long mTailPoint = 0;
    private RandomAccessFile mRaf;
    private String mPath;
    private long mLength;

    private Transform<E> transform;
    private ClearPolicy mPolicy = new DefaultClearPolicy();
    private AtomicBoolean mClear = new AtomicBoolean(false);

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
            logger("parse header. mHeadPoint = " + mHeadPoint + ", mTailPoint = " + mTailPoint + "");
            if (mHeadPoint < 0 || mTailPoint < 0 || mHeadPoint > mTailPoint) {
                logger("error file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final Object mUpdateHead = new Object();
    private final Object mUpdateTail = new Object();

    private void updateHead(long head) throws Exception {
        synchronized (mUpdateHead) {
            if (head > mHeadPoint) {
                RandomAccessFile r = createWriteRandomAccessFile(mPath, "rw");
                r.seek(POINT_HEAD);
                r.writeLong(head);
                mHeadPoint = head;
            }
        }
    }

    private void updateTail(long tail) throws Exception {
        synchronized (mUpdateTail) {
            if (tail > mTailPoint) {
                RandomAccessFile r = createWriteRandomAccessFile(mPath, "rw");
                r.seek(POINT_TAIL);
                r.writeLong(tail);
                mTailPoint = tail;
            }
        }
    }

    private long parseHead() throws Exception {
        RandomAccessFile r = createWriteRandomAccessFile(mPath, "r");
        r.seek(POINT_HEAD);
        return r.readLong();
    }

    private long parseTail() throws Exception {
        RandomAccessFile r = createWriteRandomAccessFile(mPath, "r");
        r.seek(POINT_TAIL);
        return r.readLong();
    }

    private void createWriteRandomAccessFile(String path, long length) throws IOException {
        if (this.mRaf == null) {
            this.mRaf = createWriteRandomAccessFile(path);
            mRaf.setLength(length);
        }
    }

    private void checkRandomAccessFile() throws IOException {
        createWriteRandomAccessFile(mPath, mLength);
    }

    private static @NotNull
    RandomAccessFile createWriteRandomAccessFile(String path, String mode)
            throws FileNotFoundException {
        return new RandomAccessFile(path, mode);
    }

    private static @NotNull
    RandomAccessFile createWriteRandomAccessFile(String path)
            throws FileNotFoundException {
        return createWriteRandomAccessFile(path, "rw");
    }

    // TODO ADD
    public void add(@NotNull E e) {

    }

    private void capacity() {
        logger("capacity!!!");
        mLength += DEFAULT_LENGTH;
    }

    /**
     * todo 功能：数据写入一定的length是否需要分文件存贮？利于：磁盘满了直接删除老的文件。
     */
    public void put(@NotNull E e) throws InterruptedException {
        if (mClear.get()) {
            return;
        }
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            checkRandomAccessFile();

            // todo 这里自动扩容了，下面的while不会走
            if (mLength >> 2 - mTailPoint < 0) {
                capacity();
                mRaf.setLength(mLength);
            }

            while (mTailPoint >= mLength) {
                if (checkDiskSize()) {
                    capacity();
                    mRaf.setLength(mLength);
                } else {
                    // todo 处理内存不足的情况
                    new Thread(this::tryClearDisk).start();
                    System.out.println("< Not enough disk space ! wait... ... >");
                    notFull.await();
                }
            }

            mRaf.seek(mTailPoint);
            transform.write(e, mRaf);
            long tail = mRaf.getFilePointer();
            updateTail(tail);
            onTailChanged(tail);
            signalNotEmpty();
            logger("put  mTailPoint = " + tail);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
//            if (mRaf != null) {
//                mRaf.close(); // close 时关闭
//            }
            putLock.unlock();
        }
    }

    private void tryClearDisk() {
        // stop all put and take first?
        mClear.set(true);
        boolean result = clearDisk();
        mClear.set(false);
        //if (result) {
        signalNotFull();
        //}
    }

    protected boolean clearDisk() {
        return mPolicy.clear(this);
    }

    /**
     * TODO  checkDiskSize
     */
    protected boolean checkDiskSize() {

        return true;
    }

    public E remove() {
        // TODO non blocking
        return null;
    }

    public E take() throws Exception {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        long head = this.mHeadPoint;
        try {

            while (head >= mTailPoint || mClear.get()) {
                System.out.println("< take nothing any more or in-clearing-disk,wait... ... >");
                notEmpty.await();
            }
            RandomAccessFile readRaf = createWriteRandomAccessFile(mPath);
            readRaf.seek(this.mHeadPoint);
            E read = transform.read(readRaf);
            long filePointer = readRaf.getFilePointer();
            if (read != null /*&& mTailPoint != 0 && filePointer <= mTailPoint*/) {
                updateHead(filePointer);
//                this.mHeadPoint = readRaf.getFilePointer();
                onHeadChanged(this.mHeadPoint);
            }
            logger("take mHeadPoint = " + this.mHeadPoint);
            readRaf.close();
            return read;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            takeLock.unlock();
        }
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
            RandomAccessFile rw = createWriteRandomAccessFile(mPath, "rw");
            rw.seek(mHeadPoint);
            e = transform.read(rw);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return e;
    }

    public int size() {
        // todo size?
        return 0;
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
        onTailChanged(mTailPoint);
    }

    public void setHeadPoint(long mHeadPoint) {
        this.mHeadPoint = mHeadPoint;
        onHeadChanged(mHeadPoint);
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

    private void logger(String msg) {
        if (DEBUG) System.out.println(msg);
    }

    private boolean DEBUG = true;

    public void setDebug(boolean debug) {
        this.DEBUG = debug;
    }

    @Override
    public void onHeadChanged(long head) {

    }

    @Override
    public void onTailChanged(long tail) {

    }
}
