package cache;
import java.util.Map;

public class Cache {
    private int cacheSize;
    protected Map<String, String> cacheMap;

    public Cache(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void clear() {
        this.cacheMap.clear();
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public void put(String key, String value) {
        if (value.equals("")) {
            this.cacheMap.remove(key);
        } else {
            this.cacheMap.put(key, value);
        }
    }

    public String get(String key) {
        return this.cacheMap.get(key);
    }

    public boolean containsKey(String key) {
        return this.cacheMap.containsKey(key);
    }

}
