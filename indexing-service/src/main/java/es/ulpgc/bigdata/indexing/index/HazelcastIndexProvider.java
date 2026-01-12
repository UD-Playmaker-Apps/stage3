package es.ulpgc.bigdata.indexing.index;

import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class HazelcastIndexProvider {

    private final HazelcastInstance hz;
    private final MultiMap<String, String> invertedIndex;
    private final ISet<String> indexedDocs;
    private final IMap<String, Map<String, Object>> metadataIndex;

    public HazelcastIndexProvider(String clusterName, int backupCount, int asyncBackupCount) {
        Config cfg = new Config().setClusterName(clusterName);

        // MultiMapConfig (no MapConfig) + LIST para permitir duplicados y por tanto TF real
        cfg.getMultiMapConfig("inverted-index")
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        cfg.addMapConfig(new MapConfig("metadata-index")
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount));

        this.hz = Hazelcast.newHazelcastInstance(cfg);
        this.invertedIndex = hz.getMultiMap("inverted-index");
        this.indexedDocs = hz.getSet("indexed-docs");
        this.metadataIndex = hz.getMap("metadata-index");
    }

    public HazelcastInstance hazelcast() {
        return hz;
    }

    public MultiMap<String, String> invertedIndex() {
        return invertedIndex;
    }

    public ISet<String> indexedDocs() {
        return indexedDocs;
    }

    public IMap<String, Map<String, Object>> metadataIndex() {
        return metadataIndex;
    }

    public Collection<String> getDocs(String term) {
        return invertedIndex.get(term);
    }

    public Set<String> terms() {
        return invertedIndex.keySet();
    }

    public int size() {
        return invertedIndex.size();
    }

    public void shutdown() {
        hz.shutdown();
    }
}
