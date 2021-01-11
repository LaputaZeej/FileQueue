package com.bugu.queue;

import com.bugu.queue.exception.FileQueueException;
import com.bugu.queue.exception.NotEnoughDiskException;
import com.bugu.queue.transform.Transform;
import com.bugu.queue.util.Logger;
import com.bugu.queue.util.RafUtil;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final public class FileQueue<E> {

    /**
     * Ĭ���ļ�����
     */
    //    private static final long DEFAULT_LENGTH = 2 << 19; // 1M
    private static final long DEFAULT_LENGTH = 2 << 9; // 1KB

    /**
     * ��¼ͷ��λ��
     */
    private static final long POINT_HEAD = 0;

    /**
     * ��¼β��λ��
     */
    private static final long POINT_TAIL = 2 << 2;

    /**
     * ͷ�ļ�����
     */
    static final long HEADER_LENGTH = 2 << 3;

    private long mHeadPoint = 0;
    private long mTailPoint = 0;
    private RandomAccessFile mRaf;
    private RandomAccessFile mReadRaf = null;
    private String mPath;
    private long mLength;

    private Transform<E> transform;
    private CompressPolicy mPolicy = new DefaultCompressPolicy();
    private CapacityThreshold capacityThreshold = new DefaultCapacityThreshold();
    private Checker checker = new DefaultChecker();
    private FileQueuePointerChanged pointerChanged;

    /**
     * ����ѹ���ռ�
     */
    private AtomicBoolean mCompressing = new AtomicBoolean(false);


    private RandomAccessFile mUpdateHeadRaf;
    private RandomAccessFile mUpdateTailRaf;
    private final Object mUpdateHead = new Object();
    private final Object mUpdateTail = new Object();

    private RandomAccessFile mReadTailRaf;
    private RandomAccessFile mReadHeadRaf;

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

    public FileQueue(String path, long length, Transform<E> transform) {
        this.mPath = path;
        this.transform = transform;
        this.mLength = length;
        RandomAccessFile r = null;
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
            long fileLength = file.length();
            logger("file:" + path + " , fileLength = " + fileLength);
            if (fileLength >= HEADER_LENGTH) {
                r = createReadRandomAccessFile();
                parseHeader(r);
            } else {
                r = createWriteRandomAccessFile();
                initHeader(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(r);
        }
    }

    public FileQueue(String path, Transform<E> transform) {
        this(path, DEFAULT_LENGTH, transform);
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
                if (mUpdateHeadRaf == null) {
                    mUpdateHeadRaf = createWriteRandomAccessFile();
                }
                mUpdateHeadRaf.seek(POINT_HEAD);
                mUpdateHeadRaf.writeLong(head);
                mHeadPoint = head;
            }
        }
    }

    private void updateTail(long tail) throws Exception {
        synchronized (mUpdateTail) {
            if (tail > mTailPoint) {
                if (mUpdateTailRaf == null) {
                    mUpdateTailRaf = createWriteRandomAccessFile();
                }
                mUpdateTailRaf.seek(POINT_TAIL);
                mUpdateTailRaf.writeLong(tail);
                mTailPoint = tail;
            }
        }
    }

    private long parseHead() throws Exception {
        if (mReadHeadRaf == null) {
            mReadHeadRaf = createReadRandomAccessFile();
        }
        mReadHeadRaf.seek(POINT_HEAD);
        return mReadHeadRaf.readLong();
    }

    private long parseTail() throws Exception {
        if (mReadTailRaf == null) {
            mReadTailRaf = createReadRandomAccessFile();
        }
        mReadTailRaf.seek(POINT_TAIL);
        return mReadTailRaf.readLong();
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

    // ʣ�೤����ֵ
    private long threshold() {
        return capacityThreshold.capacity(this);
    }

    public void put(@NotNull E e) throws InterruptedException {
        if (mCompressing.get()) {
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
            while (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                // ����ǰ�����ʣ����̴�С
                while (!checkDiskSize()) {
                    System.out.println("< Not enough disk space ! wait... ... >");
                    // ���̲��� ���ͷ��ļ��ж�������ݣ�head֮ǰ�����ݣ�
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

            logger("put  mTailPoint = " + tail);
            // todo test ��ӳɹ������������ӣ����������߳���ӵ���
            if (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                notFull.signal(); // ��������߳�
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
//            if (mRaf != null) {
//                mRaf.close(); // close
//            }
            putLock.unlock();
        }
        signalNotEmpty();
    }

    // todo test
    public boolean offer(@NotNull E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (mCompressing.get()) {
            System.out.println("��������....");
            return false;
        }
        long c = -1;
        final AtomicLong count = new AtomicLong(this.mLength);
        final ReentrantLock putLock = this.putLock;
        long nanos = unit.toNanos(timeout);
        putLock.lockInterruptibly();
        try {
            checkRandomAccessFile();
            // ��tail�����ļ�ĩβʱ����������
            while (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                // ����ǰ�����ʣ����̴�С
                while (!checkDiskSize()) {
                    if (nanos <= 0) {//��ʱ��������ȫ������δ�ɹ� ����false
                        return false;
                    }
                    System.out.println("< Not enough disk space ! wait... ... >");
                    // ���̲��� ���ͷ��ļ��ж�������ݣ�head֮ǰ�����ݣ�
                    tryCompressDisk();
                    nanos = notFull.awaitNanos(nanos);
                }
                capacity();
                mRaf.setLength(mLength);
            }
            long tail = enqueue(e);
            if (pointerChanged != null) {
                pointerChanged.onTailChanged(tail);
            }

            logger("put  mTailPoint = " + tail);
            // todo test ��ӳɹ������������ӣ����������߳���ӵ���
            if (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                notFull.signal(); // ��������߳�
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
//            if (mRaf != null) {
//                mRaf.close(); // close
//            }
            putLock.unlock();
        }
        signalNotEmpty();
        return true;
    }

    // todo test
    public boolean offer(@NotNull E e) throws InterruptedException {
        if (mCompressing.get()) {
            System.out.println("��������....");
            return false;
        }
        // ����ǰ�ж��Ƿ���
        if (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
            return false;
        }
        final ReentrantLock putLock = this.putLock;
        putLock.lockInterruptibly();
        try {
            checkRandomAccessFile();

            // ֱ�ӷ���false
            if (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                return false;
            }

            long tail = enqueue(e);
            if (pointerChanged != null) {
                pointerChanged.onTailChanged(tail);
            }

            // todo test ��ӳɹ������������ӣ����������߳���ӵ���
            if (mTailPoint >= mLength || mLength - threshold() < mTailPoint) {
                notFull.signal(); // ��������߳�
            }
            logger("put  mTailPoint = " + tail);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
//            if (mRaf != null) {
//                mRaf.close(); // close
//            }
            putLock.unlock();
        }
        signalNotEmpty();
        return true;
    }

    public E take() throws Exception {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        long head = this.mHeadPoint;

        try {
            while (head >= mTailPoint || mCompressing.get()) {
                System.out.println("< take nothing any more or in-compressing-disk,wait... ... >");
                notEmpty.await();
            }
            if (mReadRaf == null) {
                mReadRaf = createReadRandomAccessFile();
            }
            E e = dequeue(mReadRaf);
            long filePointer = mReadRaf.getFilePointer();
            if (e != null /*&& mTailPoint != 0 && filePointer <= mTailPoint*/) {
                updateHead(filePointer);
//                this.mHeadPoint = readRaf.getFilePointer();
                if (pointerChanged != null) {
                    pointerChanged.onHeadChanged(this.mHeadPoint);
                }
                // todo test ���ȡ�껹�� ����ȡ���߳�
                if (head >= mTailPoint || mCompressing.get()) {
                    notEmpty.signal();
                }

                // todo ��Ϊû��ɾ���������ļ�����ţ� ���Բ���֪ͨnotFull��
                // signalNotFull();
            }
            logger("take mHeadPoint = " + this.mHeadPoint);
            return e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /*if (readRaf != null) {
                readRaf.close();
            }*/
            takeLock.unlock();
        }
        throw new FileQueueException("take null");
    }

    // todo test
    public E poll(long timeout, TimeUnit unit) throws Exception {
        final ReentrantLock takeLock = this.takeLock;
        long nanos = unit.toNanos(timeout);
        takeLock.lockInterruptibly();
        long head = this.mHeadPoint;
        RandomAccessFile readRaf = null;
        try {
            while (head >= mTailPoint || mCompressing.get()) {
                System.out.println("< take nothing any more or in-compressing-disk,wait... ... >");
                if (nanos <= 0) {//ʱ��������ȫ���������δ�ɹ��򷵻�null
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
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
            close(readRaf);
            takeLock.unlock();
        }
        return null;
    }

    // todo test
    public E poll() throws InterruptedException {
        long head = this.mHeadPoint;
        if (head >= mTailPoint || mCompressing.get()) {
            return null;
        }
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        RandomAccessFile readRaf = null;
        try {
            if (head >= mTailPoint || mCompressing.get()) {
                return null;
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
            close(readRaf);
            takeLock.unlock();
        }
        return null;
    }

    public E peek() {
        if (mHeadPoint >= mTailPoint) {
            return null;
        }
        E e = null;
        RandomAccessFile rw = null;
        try {
            rw = createReadRandomAccessFile();
            e = dequeue(rw);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(rw);
        }
        return e;
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
                // todo ֹͣ��
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

    protected boolean checkDiskSize() {
        return checker.hasDisk(this);
    }

    public E remove() {
        // TODO non blocking
        return null;
    }


    public void close(RandomAccessFile raf) {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        close(mRaf);
        close(mReadRaf);
        close(mReadHeadRaf);
        close(mReadTailRaf);
        close(mUpdateHeadRaf);
        close(mUpdateTailRaf);
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

    public void setChecker(Checker checker) {
        this.checker = checker;
    }

    public void setCompressing(AtomicBoolean mCompressing) {
        this.mCompressing = mCompressing;
    }

    public void setCapacityThreshold(CapacityThreshold capacityThreshold) {
        this.capacityThreshold = capacityThreshold;
    }

    private void logger(String msg) {
        Logger.logger(msg);
    }


}
