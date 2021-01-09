package com.bugu.queue;

import com.bugu.queue.exception.FileQueueException;
import com.bugu.queue.exception.NotEnoughDiskException;
import com.bugu.queue.transform.Transform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final public class FileQueue<E> implements PointerChanged {

    //    private static final long DEFAULT_LENGTH = 2 << 19; // 1M
    private static final long DEFAULT_LENGTH = 2 << 9; // 1KB
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

    private final Object mUpdateHead = new Object();
    private final Object mUpdateTail = new Object();

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

    // TODO ADD
    public void add(@NotNull E e) {

    }

    private void capacity() {
        logger("capacity!!!");
        mLength += DEFAULT_LENGTH;
    }

    // ʣ�೤����ֵ
    private long vpt() {
        return mLength >> 3;
    }

    /**
     * todo
     */
    public void put(@NotNull E e) throws InterruptedException {
        if (mClear.get()) {
            System.out.println("��������....");
            return;
        }
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            checkRandomAccessFile();
            // todo �Զ�����
           /* if (mLength - vpt() < mTailPoint) {
                capacity();
                mRaf.setLength(mLength);
            }*/

            // ��tail�����ļ�ĩβʱ����������
            while (mTailPoint >= mLength || mLength - vpt() < mTailPoint) {
                // ����ǰ�����ʣ����̴�С
                while (!checkDiskSize()) {
                    System.out.println("< Not enough disk space ! wait... ... >");
                    // ���̲��� ���ͷ��ļ��ж�������ݣ�head֮ǰ�����ݣ�
                    tryClearDisk();
                    notFull.await();
                }
                capacity();
                mRaf.setLength(mLength);
            }

            long tail = enqueue(e);
            onTailChanged(tail);
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

    private E dequeue(RandomAccessFile readRaf) throws Exception {
        readRaf.seek(this.mHeadPoint);
        return transform.read(readRaf);
    }

    private long enqueue(@NotNull E e) throws Exception {
        mRaf.seek(mTailPoint);
        transform.write(e, mRaf);
        long tail = mRaf.getFilePointer();
        updateTail(tail);
        return tail;
    }

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private void beforeClearForTest() {
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

    private long clearDiskTimeOutTime = 1000;

    private void tryClearDisk() throws NotEnoughDiskException {
        // stop all put and take first?
        executorService.submit(() -> {
            // beforeClearForTest();
            mClear.set(true);
            boolean result = clearDisk();
            mClear.set(false);
            System.out.println("clear result = " + result);
            if (result) {
                signalNotFull();
                signalNotEmpty();
            } else {
                // throw new NotEnoughDiskException();
                // todo ֹͣ��
                try {
                    clearDiskTimeOutTime = clearDiskTimeOutTime * 2;
                    if (clearDiskTimeOutTime > 100 * 1000) {
                        clearDiskTimeOutTime = 1000;
                    }
                    Thread.sleep(clearDiskTimeOutTime);
                    signalNotFull();
                    signalNotEmpty();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    public boolean clearDisk() {
        return mPolicy.clear(this);
    }

    /**
     * TODO  checkDiskSize
     */
    protected boolean checkDiskSize() {
        try {
            long length = mRaf.length();
            return length <= DEFAULT_LENGTH * 512;
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        RandomAccessFile readRaf = null;
        try {
            while (head >= mTailPoint /*|| mClear.get()*/) {
                System.out.println("< take nothing any more or in-clearing-disk,wait... ... >");
                notEmpty.await();
            }
            readRaf = createReadRandomAccessFile();
            E e = dequeue(readRaf);
            long filePointer = readRaf.getFilePointer();
            if (e != null /*&& mTailPoint != 0 && filePointer <= mTailPoint*/) {
                updateHead(filePointer);
//                this.mHeadPoint = readRaf.getFilePointer();
                onHeadChanged(this.mHeadPoint);
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
