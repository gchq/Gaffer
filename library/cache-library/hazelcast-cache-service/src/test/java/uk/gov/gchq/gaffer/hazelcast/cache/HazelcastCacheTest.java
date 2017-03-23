package uk.gov.gchq.gaffer.hazelcast.cache;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HazelcastCacheTest {


    private static HazelcastCache<String, Integer> cache;

    @BeforeClass
    public static void setUp() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<String, Integer> map = instance.getMap("test");

        cache = new HazelcastCache<>(map);
    }

    @Before
    public void before() {
        cache.clear();
    }

    @Test
    public void shouldThrowAnExceptionIfEntryAlreadyExistsWhenUsingPutSafe(){
        cache.put("test", 1);
        try {
            cache.putSafe("test", 1);
            fail();
        } catch (CacheOperationException e) {
            assertEquals("Entry for key test already exists", e.getMessage());
        }
    }

}
