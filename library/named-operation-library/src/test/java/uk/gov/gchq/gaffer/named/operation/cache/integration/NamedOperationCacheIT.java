package uk.gov.gchq.gaffer.named.operation.cache.integration;


import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.impl.HazelcastCacheService;
import uk.gov.gchq.gaffer.cache.impl.JcsCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class NamedOperationCacheIT
{
    private static Graph graph;
    private static final String CACHE_NAME = "NamedOperation";
    private Properties cacheProps = new Properties();

    private AddNamedOperation add = new AddNamedOperation.Builder()
            .name("op")
            .description("test operation")
            .operationChain(new OperationChain.Builder()
                    .first(new GetAllElements.Builder()
                            .build())
                    .build())
            .build();

    private User user = new User("user01");

    @BeforeClass
    public static void setUp() throws ClassNotFoundException, StoreException, IllegalAccessException, InstantiationException {
        final StoreProperties storeProps = StoreProperties.loadStoreProperties(StreamUtil.storeProps(NamedOperationCacheIT.class));
        Store store = Class.forName(storeProps.getStoreClass()).asSubclass(Store.class).newInstance();
        store.initialise(new Schema(), storeProps);
        graph = new Graph.Builder()
                .store(store)
                .build();
    }

    @Before
    public void before() {
        cacheProps.clear();
    }

    @After
    public void after() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
    }

    @Test
    public void shouldWorkUsingHashMapServiceClass() throws OperationException, CacheOperationException {
        reInitialiseCacheService(HashMapCacheService.class);
        runTests();
    }

    @Test
    public void shouldWorkUsingJCSWithNoConfig() throws OperationException, CacheOperationException {
        reInitialiseCacheService(JcsCacheService.class);
        runTests();
    }

    @Test
    public void shouldWorkUsingHazelcastWithNoConfig() throws OperationException, CacheOperationException {
        reInitialiseCacheService(HazelcastCacheService.class);
        runTests();
    }

    private void reInitialiseCacheService(Class clazz) {
        cacheProps.setProperty(CacheProperties.CACHE_SERVICE_CLASS, clazz.getCanonicalName());
        CacheServiceLoader.initialise(cacheProps);
    }

    private void runTests() throws OperationException, CacheOperationException {
        shouldAllowUpdatingOfNamedOperations();
        after();
        shouldBeAbleToAddNamedOperationToCache();
        after();
        shouldBeAbleToDeleteNamedOperationFromCache();
    }


    private void shouldBeAbleToAddNamedOperationToCache() throws OperationException {

        // given
        GetAllNamedOperations get = new GetAllNamedOperations.Builder().build();

        // when
        graph.execute(add, user);

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .operationChain(add.getOperationChain())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .description(add.getDescription())
                .build();

        List<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        List<NamedOperationDetail> results = Lists.newArrayList(graph.execute(get, user));

        // then
        assertEquals(1, results.size());
        assertEquals(expected, results);
    }


    private void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        graph.execute(add, user);

        DeleteNamedOperation del = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        graph.execute(del, user);

        List<NamedOperationDetail> results = Lists.newArrayList(graph.execute(get, user));

        // then
        assertEquals(0, results.size());

    }


    private void shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // given
        graph.execute(add, user);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChain())
                .overwrite()
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        graph.execute(update, user);

        List<NamedOperationDetail> results = Lists.newArrayList(graph.execute(get, user));

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChain())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .build();

        ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);

        // then
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }
}
