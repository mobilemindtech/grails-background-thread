import grails.util.GrailsUtil;
import org.apache.log4j.Logger;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.orm.hibernate3.SessionFactoryUtils;
import org.springframework.orm.hibernate3.SessionHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class BackgroundThreadManager {

    private static final Logger log = Logger.getLogger(BackgroundThreadManager.class);

    private BlockingQueue<Runnable> queue;

    private final AtomicInteger threadCount = new AtomicInteger(1);

    private final AtomicInteger tasksPerDrain = new AtomicInteger(1);

    private SessionFactory sessionFactory;

    private final AtomicBoolean stop = new AtomicBoolean(false);

    private ThreadFactory threadFactory;

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    @Required
    public void setSessionFactory(SessionFactory newSessionFactory) {
        this.sessionFactory = newSessionFactory;
    }

    public int getTasksPerDrain() {
        return tasksPerDrain.get();
    }

    public void setTasksPerDrain(int newTasksPerDrain) {
        tasksPerDrain.set(newTasksPerDrain);
    }

    public int getThreadCount() {
        return threadCount.get();
    }

    public void setThreadCount(final int newThreadCount) {
        threadCount.set(newThreadCount);
    }

    public synchronized BlockingQueue<Runnable> getQueue() {
        return queue;
    }

    @Required
        public synchronized void setQueue(BlockingQueue<Runnable> newQueue) {
    queue = newQueue;
    }

    public synchronized ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    @Required
    public synchronized void setThreadFactory(final ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    protected synchronized Runnable createRunnable() {
        if(queue == null) {
            throw new IllegalStateException("No queue property provided");
        }
    
        final int size = getTasksPerDrain();
        final List<Runnable> tasks = new LinkedList<Runnable>();
        
        return new Runnable() {
            public void run() {
                boolean boundByMe = false;
                try {
                    log.info("Starting " + Thread.currentThread().getName());
                    while(!stop.get()) {
                        final Runnable runMe = nextRunnable(tasks, size);
                        if(stop.get()) { break; }
                        try {
                            boundByMe = bindSession();
                            runMe.run();
                            final SessionHolder sessionHolder = (SessionHolder) TransactionSynchronizationManager.getResource(sessionFactory);
                            if(sessionHolder != null && !FlushMode.MANUAL.equals(sessionHolder.getSession().getFlushMode())) {
                                sessionHolder.getSession().flush();
                            }
                        } catch(Exception e) { 
                            fireThreadException(e); 
                        } finally {
                            if(boundByMe) unbindSession(); 
                        }
                    }
                } finally {
                    log.info("Shutting down " + Thread.currentThread().getName());
                    if(!stop.get()) {
                        log.warn("Starting new thread because stop is not signaled");
                        createNewQueueThread().start();
                    }
                }
            }
        };
    }

    private Runnable nextRunnable(final List<Runnable> tasks, final int size) {
        try {
            if(tasks.isEmpty() && queue.drainTo(tasks, size) == 0) {
                return queue.take();
            } else {
                return tasks.remove(0);
            }
        } catch(RuntimeException re) {
            throw re;
        } catch(Exception e) {
            throw new RuntimeException("Error while retrieving next task", e);
        }
    }

    private boolean bindSession() {
        if(sessionFactory == null) {
            throw new IllegalStateException("No sessionFactory property provided");
        }
        final Object inStorage = TransactionSynchronizationManager.getResource(sessionFactory);
        if(inStorage != null) {
            ((SessionHolder)inStorage).getSession().flush();
            return false;
        } else {
            Session session = SessionFactoryUtils.getSession(sessionFactory, true);
            session.setFlushMode(FlushMode.AUTO);
            TransactionSynchronizationManager.bindResource(sessionFactory, new SessionHolder(session));
            return true;
        }
    }

    private void unbindSession() {
        if(sessionFactory == null) {
            throw new IllegalStateException("No sessionFactory property provided");
        }
        SessionHolder sessionHolder = (SessionHolder)TransactionSynchronizationManager.getResource(sessionFactory);
        try {
            if(!FlushMode.MANUAL.equals(sessionHolder.getSession().getFlushMode())) {
                log.info("FLUSHING SESSION IN BACKGROUND");
                sessionHolder.getSession().flush();
            }
        } catch(Exception e) {
            fireThreadException(e);
        } finally {
            TransactionSynchronizationManager.unbindResource(sessionFactory);
            SessionFactoryUtils.closeSession(sessionHolder.getSession());
        }
    }

    protected synchronized Thread createNewThread(final Runnable r) {
        if(threadFactory == null) {
            throw new IllegalStateException("No thread factory provided to manager");
        }
        final Thread thread = threadFactory.newThread(r);
        if(missesExplicitExceptionHandler(thread)) {
            throw new IllegalStateException("No handler property provided for the current background worker thread");
        }
        return thread;
    }

    protected synchronized Thread createNewQueueThread() {
        return createNewThread(createRunnable());
    }

    public void start() {
        //todo Maybe we should prevent subsequent calls to start()
        if (stop.get())
            throw new IllegalStateException("Cannot start an already stopped thread pool.");
        for (int i = 0; i < getThreadCount(); i++) {
            createNewQueueThread().start();
        }
    }

    public void stop() {
        //todo Maybe all threads in the pool should be stopped/interrupted so that they don't wait forever, maybe using poll() with a timeout
        stop.set(true);
    }

    public void queueRunnable(final Runnable r) {
        try {
            queue.put(r); // InterruptedException
        } catch(InterruptedException ie) {
            GrailsUtil.deepSanitize(ie);
            log.error("Aborting putting " + r.getClass().getSimpleName(), ie);
        }
    }

    /**
     * Checks whether the current thread has an exception handler set, which is different from the implicit one
     * (i.e. thread's thread group)
     * @param thread The thread to investigate
     * @return True if the thead doesn't have an explicit exception handler set
     */
    private static boolean missesExplicitExceptionHandler(final Thread thread) {
        return (thread.getUncaughtExceptionHandler() == null);
    }

    private static void fireThreadException(final Exception e) {
        final Thread thread = Thread.currentThread();
        if (missesExplicitExceptionHandler(thread)) {
            //Logging the problem that the current thread doesn't have an uncaught exception handler set.
            //Bare throwing an exception might not have any effect in such a case.
            final String message = "No handler property provided for the current background worker thread " + thread.getName()
                    + " when trying to handle an exception. ";
            log.error(message, e);
        } else {
            thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
        }
    }
}

