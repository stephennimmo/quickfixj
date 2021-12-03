package quickfix;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Message store using Infinispan in-memory data grid for message and
 * sequence number storage.
 */
public class InfinispanStoreFactory implements MessageStoreFactory {

    /**
     * File path where the Infinispan Hot Rod Client properties file is located. Required.
     */
    public static final String SETTING_INFINISPAN_HOT_ROD_CLIENT_PROPERTIES_PATH = "InfinispanHotRodClientPropertiesPath";

    private final SessionSettings settings;

    public InfinispanStoreFactory(SessionSettings settings) {
        this.settings = settings;
    }

    @Override
    public MessageStore create(SessionID sessionID) {
        try {
            String propertiesPath = settings.getString(sessionID, SETTING_INFINISPAN_HOT_ROD_CLIENT_PROPERTIES_PATH);
            ConfigurationBuilder builder = new ConfigurationBuilder();
            Properties p = new Properties();
            try(Reader r = new FileReader(propertiesPath)) {
                p.load(r);
                builder.withProperties(p);
            }
            RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
            return new InfinispanStore(sessionID, remoteCacheManager);
        } catch (ConfigError e) {
            throw new RuntimeError(e);
        } catch (IOException e) {
            throw new RuntimeError(e);
        }
    }
}
