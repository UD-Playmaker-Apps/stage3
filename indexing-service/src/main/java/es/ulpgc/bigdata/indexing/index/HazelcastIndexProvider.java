package es.ulpgc.bigdata.indexing.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;

import java.util.Collection;

public class HazelcastIndexProvider {

    private final HazelcastInstance hz;
    private final MultiMap<String, String> invertedIndex;

    public HazelcastIndexProvider(String clusterName, int backupCount, int asyncBackupCount) {
        Config cfg = new Config().setClusterName(clusterName);
        cfg.addMapConfig(new MapConfig("inverted-index")
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount));
        this.hz = Hazelcast.newHazelcastInstance(cfg);
        this.invertedIndex = hz.getMultiMap("inverted-index");
    }

    public MultiMap<String,String> invertedIndex() { return invertedIndex; }
    public HazelcastInstance hazelcast() { return hz; }
    public int size() { return invertedIndex.size(); }
    public Collection<String> getDocs(String term) { return invertedIndex.get(term); }
    public void shutdown() { hz.shutdown(); }
}
