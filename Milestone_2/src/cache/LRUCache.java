package cache;
import java.util.Map;
import java.util.LinkedHashMap;

// Least recently used
public class LRUCache extends Cache{
    public LRUCache(Integer cacheSize) {
        super(cacheSize);
        // capacity, load factor (set as default), ordering mode (true for access-order, false for insertion-order)
        this.cacheMap = new LinkedHashMap<String, String>(this.getCacheSize(), (float) 0.75, true){
            // https://www.geeksforgeeks.org/linkedhashmap-removeeldestentry-method-in-java/
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > getCacheSize();
            }
        };
    }
}
