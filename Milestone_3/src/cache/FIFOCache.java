package cache;
import java.util.Map;
import java.util.LinkedHashMap;

// First in first out
public class FIFOCache extends Cache{
    public FIFOCache(Integer cacheSize) {
        super(cacheSize);
        // capacity, load factor (set as default), ordering mode (true for access-order, false for insertion-order)
        this.cacheMap = new LinkedHashMap<String, String>(this.getCacheSize(), (float) 0.75, false){
            // https://www.geeksforgeeks.org/linkedhashmap-removeeldestentry-method-in-java/
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > getCacheSize();
            }
        };
    }
}
