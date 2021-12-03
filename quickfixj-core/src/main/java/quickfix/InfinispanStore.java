package quickfix;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class InfinispanStore implements MessageStore {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final SessionID sessionID;
    private final RemoteCacheManager remoteCacheManager;
    private final CounterManager counterManager;
    private final RemoteCache<Integer, String> remoteCache;
    private final StrongCounter nextSenderMsgSeqNumCounter;
    private final StrongCounter nextTargetMsgSeqNumCounter;

    public InfinispanStore(SessionID sessionID, RemoteCacheManager remoteCacheManager) {
        this.sessionID = sessionID;
        this.remoteCacheManager = remoteCacheManager;
        this.counterManager = RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
        String cacheName = String.format("%s-%s","message-store", sessionID);
        this.remoteCache = this.remoteCacheManager.administration().getOrCreateCache(cacheName, DefaultTemplate.DIST_ASYNC);
        this.remoteCache.put(-1, Long.toString(SystemTime.getUtcCalendar().getTimeInMillis()));
        String nextSenderMsgSeqNumCounterName = String.format("%s-%s","NextSenderMsgSeqNumCounter", sessionID);
        this.nextSenderMsgSeqNumCounter = this.counterManager.getStrongCounter(nextSenderMsgSeqNumCounterName);
        String nextTargetMsgSeqNumCounterName = String.format("%s-%s","NextTargetMsgSeqNumCounter", sessionID);
        this.nextTargetMsgSeqNumCounter = this.counterManager.getStrongCounter(nextTargetMsgSeqNumCounterName);
    }

    @Override
    public void get(int startSequence, int endSequence, Collection<String> messages) throws IOException {
        for (int i = startSequence; i <= endSequence; i++) {
            String message = this.remoteCache.get(i);
            if (message != null) {
                messages.add(message);
            }
        }
    }

    @Override
    public boolean set(int sequence, String message) throws IOException {
        return this.remoteCache.put(sequence, message) == null;
    }

    @Override
    public int getNextSenderMsgSeqNum() throws IOException {
        return (int) this.nextSenderMsgSeqNumCounter.sync().getValue();
    }

    @Override
    public void setNextSenderMsgSeqNum(int next) throws IOException {
        this.nextSenderMsgSeqNumCounter.sync().compareAndSet(next, next);
    }

    @Override
    public void incrNextSenderMsgSeqNum() throws IOException {
        this.nextSenderMsgSeqNumCounter.incrementAndGet();
    }

    @Override
    public int getNextTargetMsgSeqNum() throws IOException {
        return (int) this.nextTargetMsgSeqNumCounter.sync().getValue();
    }

    @Override
    public void setNextTargetMsgSeqNum(int next) throws IOException {
        this.nextTargetMsgSeqNumCounter.sync().compareAndSet(next, next);
    }

    @Override
    public void incrNextTargetMsgSeqNum() throws IOException {
        this.nextTargetMsgSeqNumCounter.incrementAndGet();
    }

    @Override
    public Date getCreationTime() throws IOException {
        return new Date(Long.parseLong(this.remoteCache.get(-1)));
    }

    @Override
    public void reset() throws IOException {
        this.setNextSenderMsgSeqNum(1);
        this.setNextTargetMsgSeqNum(1);
        long createTime = Long.parseLong(this.remoteCache.get(-1));
        this.remoteCache.clear();
        this.remoteCache.put(-1, Long.toString(createTime));
    }

    @Override
    public void refresh() throws IOException {
        // IOException is declared to maintain strict compatibility with QF JNI
        final String text = "Infinispan store does not support refresh!";
        final Session session = sessionID != null ? Session.lookupSession(sessionID) : null;
        if (session != null) {
            session.getLog().onErrorEvent("ERROR: " + text);
        } else {
            LoggerFactory.getLogger(InfinispanStore.class).error(text);
        }
    }

}
