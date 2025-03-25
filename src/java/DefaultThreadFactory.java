import org.springframework.beans.factory.annotation.Required;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a standard way to create new threads. The created threads get their daemons flags set as well as
 * their priority is set to Thread.MIN_PRIORITY.
 * Instance of this thread factory is used by the BackgroundThreadManager by default. You can supply your own
 * thread factory by altering the resources.groovy file the following way:
 * <pre>
 * bgThreadFactory(my.package.MyThreadFactory)
 * </pre>
 * Bear in mind that all created threads are required to have the uncaughtExceptionHandler property set
 * by the thread factory.
 */
public final class DefaultThreadFactory implements ThreadFactory {

  private final AtomicLong threadIdSource = new AtomicLong(1L);

    private Thread.UncaughtExceptionHandler handler;

    public synchronized Thread.UncaughtExceptionHandler getExceptionHandler() {
      return handler;
    }
    @Required
    public synchronized void setExceptionHandler(Thread.UncaughtExceptionHandler newHandler) {
      handler = newHandler;
    }

    public Thread newThread(final Runnable r) {
        if (r == null) throw new IllegalArgumentException("Cannot create thread for a null Runnable object");
        if(handler == null) throw new IllegalStateException("Need an exception handler");
        final Thread out = new Thread(r);
        out.setDaemon(true);
        out.setName(BackgroundThreadManager.class.getSimpleName() + " Thread (BG#" + threadIdSource.getAndIncrement() + ")");
        out.setPriority(Thread.MIN_PRIORITY);
        out.setUncaughtExceptionHandler(handler);
        return out;
    }
}
