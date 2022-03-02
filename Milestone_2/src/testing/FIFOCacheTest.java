package testing;

import junit.framework.TestCase;
import org.junit.Test;
import cache.FIFOCache;

public class FIFOCacheTest extends TestCase {
    private FIFOCache cache;

    public void setUp() {
        try {
            this.cache = new FIFOCache(5);
            this.cache.clear();
        } catch (Exception e) {
            System.out.println("Failed to set up FIFO cache");
            System.out.println(e);
        }
    }

    @Test
    public void testFIFOCache() {
        assertTrue(this.cache.getCacheSize() == 5);
        assertTrue(!this.cache.containsKey("keyThatShouldNotExist"));
        assertNull(this.cache.get("keyThatAlsoShouldNotExist"));
        this.cache.put("abc", "pqr");
        this.cache.put("def", "stu");
        this.cache.put("ghi", "vwx");
        this.cache.put("jkl", "yza");
        this.cache.put("mno", "bcd");
        assertTrue(this.cache.containsKey("abc"));
        assertTrue(this.cache.containsKey("def"));
        assertTrue(this.cache.containsKey("ghi"));
        assertTrue(this.cache.containsKey("jkl"));
        assertTrue(this.cache.containsKey("mno"));
        String value = this.cache.get("abc");
        assertEquals(value, "pqr");
        value = this.cache.get("def");
        assertEquals(value, "stu");
        value = this.cache.get("ghi");
        assertEquals(value, "vwx");
        value = this.cache.get("jkl");
        assertEquals(value, "yza");
        value = this.cache.get("mno");
        assertEquals(value, "bcd");
        this.cache.put("grape", "yummy");
        assertFalse(this.cache.containsKey("abc"));
        assertTrue(this.cache.containsKey("grape"));
    }
}
