package quickfix;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

public class InfinispanStore implements MessageStore {

    private static final Logger log = LoggerFactory.getLogger(InfinispanStore.class);
    private static final int CREATION_TIME_KEY = 0;
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
        this.remoteCacheManager.getCache("test");
        String cacheName = String.format("%s-%s","message-store", sessionID);
        this.remoteCache = this.remoteCacheManager.administration().getOrCreateCache(cacheName, DefaultTemplate.DIST_ASYNC);
        this.remoteCache.put(CREATION_TIME_KEY, Long.toString(SystemTime.getUtcCalendar().getTimeInMillis()));
        String nextSenderMsgSeqNumCounterName = String.format("%s-%s","NextSenderMsgSeqNumCounter", sessionID);
        this.counterManager.defineCounter(nextSenderMsgSeqNumCounterName, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(1).build());
        this.nextSenderMsgSeqNumCounter = this.counterManager.getStrongCounter(nextSenderMsgSeqNumCounterName);
        String nextTargetMsgSeqNumCounterName = String.format("%s-%s","NextTargetMsgSeqNumCounter", sessionID);
        this.counterManager.defineCounter(nextTargetMsgSeqNumCounterName, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(1).build());
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
        this.nextSenderMsgSeqNumCounter.sync().compareAndSet(this.nextSenderMsgSeqNumCounter.sync().getValue(), next);
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
        this.nextTargetMsgSeqNumCounter.sync().compareAndSet(this.nextTargetMsgSeqNumCounter.sync().getValue(), next);
    }

    @Override
    public void incrNextTargetMsgSeqNum() throws IOException {
        this.nextTargetMsgSeqNumCounter.incrementAndGet();
    }

    @Override
    public Date getCreationTime() throws IOException {
        return new Date(Long.parseLong(this.remoteCache.get(CREATION_TIME_KEY)));
    }

    @Override
    public void reset() throws IOException {
        this.nextSenderMsgSeqNumCounter.sync().reset();
        this.nextTargetMsgSeqNumCounter.sync().reset();
        long createTime = Long.parseLong(this.remoteCache.get(CREATION_TIME_KEY));
        this.remoteCache.clear();
        this.remoteCache.put(CREATION_TIME_KEY, Long.toString(createTime));
    }

    @Override
    public void refresh() throws IOException {

    }

}
