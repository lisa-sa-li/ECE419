package testing;

import junit.framework.TestCase;
import org.junit.Test;
import cache.LFUCache;

public class LFUCacheTest extends TestCase {
    private LFUCache cache;

    public void setUp() {
        try {
            this.cache = new LFUCache(5);
            this.cache.clear();
        } catch (Exception e) {
            System.out.println("Failed to set up FIFO cache");
            System.out.println(e);
        }
    }

    @Test
    public void testLFUCache() {
        assertTrue(this.cache.getCacheSize() == 5);
        assertTrue(!this.cache.containsKey("keyThatShouldNotExist"));
        this.cache.put("1", "pqr");
        this.cache.put("2", "stu");
        this.cache.put("3", "vwx");
        assertTrue(this.cache.get("3").equals("vwx"));
        this.cache.put("4", "yza");
        this.cache.put("5", "bcd");
        assertTrue(this.cache.get("5").equals("bcd"));
        this.cache.put("1", "abc");
        this.cache.put("3", "def");

        this.cache.put("6", "yes");
        assertTrue(this.cache.containsKey("1"));
        assertFalse(this.cache.containsKey("2"));
        assertTrue(this.cache.containsKey("3"));
        assertTrue(this.cache.containsKey("4"));
        assertTrue(this.cache.containsKey("5"));
        assertTrue(this.cache.containsKey("6"));

        this.cache.put("orange", "sour");
        assertTrue(this.cache.containsKey("1"));
        assertTrue(this.cache.containsKey("3"));
        assertFalse(this.cache.containsKey("4"));
        assertTrue(this.cache.containsKey("5"));
        assertTrue(this.cache.containsKey("6"));

    }
}
