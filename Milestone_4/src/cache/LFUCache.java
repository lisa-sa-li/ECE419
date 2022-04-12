package cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

// Least frequently used
public class LFUCache extends Cache {
    private HashMap<String, Integer> frequencyMap;

    public LFUCache(Integer cacheSize) {
        super(cacheSize);
        this.cacheMap = new HashMap<String, String>(this.getCacheSize());
        this.frequencyMap = new HashMap<String, Integer>(this.getCacheSize());
    }

    public void organizeBasedOnFrequency(String key) {
        int counter;
        if (this.getCacheSize() == 0) {
            return;
        } else if (this.cacheMap.containsKey(key)) {
            // if the key exists in cache, increase the counter value
            counter = this.frequencyMap.get(key);
            counter += 1;
            this.frequencyMap.put(key, counter);
            return;
        } else if (this.cacheMap.size() >= this.getCacheSize()) {
            // Remove the least frequently used key from the cache
            String leastFrequentlyUsedKey;
            Iterator iteratorFrequency = this.frequencyMap.entrySet().iterator();
            Map.Entry entry;
            entry = (Map.Entry) iteratorFrequency.next();
            counter = (int) entry.getValue();
            leastFrequentlyUsedKey = (String) entry.getKey();
            while (iteratorFrequency.hasNext()) {
                entry = (Map.Entry) iteratorFrequency.next();
                if ((int) entry.getValue() < counter) {
                    counter = (int) entry.getValue();
                    leastFrequentlyUsedKey = (String) entry.getKey();
                }
            }
            this.cacheMap.remove(leastFrequentlyUsedKey);
            this.frequencyMap.remove(leastFrequentlyUsedKey);
        }
        counter = 1;
        this.frequencyMap.put(key, counter);
        return;
    }

    @Override
    public void put(String key, String value) {
        organizeBasedOnFrequency(key);
        super.put(key, value);
    }

    @Override
    public String get(String key) {
        organizeBasedOnFrequency(key);
        return super.get(key);
    }

}
