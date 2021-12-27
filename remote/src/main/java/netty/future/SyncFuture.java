package netty.future;

import java.util.concurrent.*;

/**
 * @author Zexho
 * @date 2021/12/27 2:42 PM
 */
public class SyncFuture<T> implements Future<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    /**
     * 需要响应线程设置的响应结果
     */
    private T response;
    /**
     * future的请求时间
     */
    private final long beginTime = System.currentTimeMillis();

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return response == null;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        latch.await();
        return this.response;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);
        return this.response;
    }

    public void setResponse(T resp) {
        this.response = resp;
        latch.countDown();
    }

    public long getBeginTime() {
        return beginTime;
    }
}
