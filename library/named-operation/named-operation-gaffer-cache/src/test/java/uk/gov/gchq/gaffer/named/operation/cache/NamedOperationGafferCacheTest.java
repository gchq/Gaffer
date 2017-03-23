package uk.gov.gchq.gaffer.named.operation.cache;


import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;

public class NamedOperationGafferCacheTest {

    private static NamedOperationGafferCache cache;

    @BeforeClass
    public static void setUp() {
        CacheServiceLoader.initialise();
        cache = new NamedOperationGafferCache();
    }
//
//    @Test
//    public void shouldAddToCache() {
//        cache.addToCache("");
//    }


}
