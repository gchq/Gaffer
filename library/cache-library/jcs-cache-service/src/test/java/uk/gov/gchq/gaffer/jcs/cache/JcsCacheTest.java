package uk.gov.gchq.gaffer.jcs.cache;


import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JcsCacheTest {

    private static JcsCache<String, Integer> cache;

    @BeforeClass
    public static void setUp() throws CacheException {
        CompositeCacheManager manager = CompositeCacheManager.getInstance();
        cache = new JcsCache<>(manager.getCache("test"));
    }

    @Before
    public void before() throws CacheOperationException {
        cache.clear();
    }


    @Test
    public void shouldThrowAnExceptionIfEntryAlreadyExistsWhenUsingPutSafe(){
        try {
            cache.put("test", 1);
            cache.putSafe("test", 1);
            fail();
        } catch (CacheOperationException e) {
            assertEquals("Entry for key test already exists", e.getMessage());
        }
    }
}
