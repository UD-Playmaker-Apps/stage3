package es.ulpgc.bigdata.indexing.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.ISet;
import com.hazelcast.multimap.MultiMap;

import java.util.Collection;
import java.util.Set;

public class HazelcastIndexProvider {

    private final HazelcastInstance hz;
    private final MultiMap<String, String> invertedIndex;
    private final ISet<String> indexedDocs;

    public HazelcastIndexProvider(String clusterName, int backupCount, int asyncBackupCount) {
        Config cfg = new Config().setClusterName(clusterName);
        cfg.addMapConfig(new MapConfig("inverted-index")
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount)
        );
        this.hz = Hazelcast.newHazelcastInstance(cfg);
        this.invertedIndex = hz.getMultiMap("inverted-index");
        this.indexedDocs = hz.getSet("indexed-docs");
    }

    public MultiMap<String, String> invertedIndex() {
        return invertedIndex;
    }

    public HazelcastInstance hazelcast() {
        return hz;
    }

    public int size() {
        return invertedIndex.size();
    }

    public Collection<String> getDocs(String term) {
        return invertedIndex.get(term);
    }

    public ISet<String> indexedDocs() {
        return indexedDocs;
    }

    public Set<String> terms() {
        return invertedIndex.keySet();
    }

    public void shutdown() {
        hz.shutdown();
    }
}
