package uk.gov.gchq.gaffer.integration.operation.named.cache;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class NamedOperationCacheIT {
    private static final String CACHE_NAME = "NamedOperation";
    private Properties cacheProps = new Properties();

    private AddNamedOperation add = new AddNamedOperation.Builder()
            .name("op")
            .description("test operation")
            .operationChain(new OperationChain.Builder()
                    .first(new GetAllElements.Builder()
                            .build())
                    .build())
            .overwrite()
            .build();

    private User user = new User();
    private Context context = new Context(user);
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler();
    private AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler();
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler1 = new GetAllNamedOperationsHandler();
    private DeleteNamedOperationHandler deleteNamedOperationHandler = new DeleteNamedOperationHandler();

    @Before
    public void before() throws CacheOperationException {
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

    private void reInitialiseCacheService(final Class clazz) throws CacheOperationException {
        cacheProps.setProperty(CacheProperties.CACHE_SERVICE_CLASS, clazz.getCanonicalName());
        CacheServiceLoader.initialise(cacheProps);
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
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
        final Store store = mock(Store.class);

        // when
        addNamedOperationHandler.doOperation(add, context, store);

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .operationChain(add.getOperationChainAsString())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .description(add.getDescription())
                .build();

        List<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        List<NamedOperationDetail> results = Lists.newArrayList(new GetAllNamedOperationsHandler().doOperation(get, context, store));

        // then
        assertEquals(1, results.size());
        assertEquals(expected, results);
    }


    private void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        final Store store = mock(Store.class);
        new AddNamedOperationHandler().doOperation(add, context, store);

        DeleteNamedOperation del = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        deleteNamedOperationHandler.doOperation(del, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler1.doOperation(get, context, store));

        // then
        assertEquals(0, results.size());

    }


    private void shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // given
        final Store store = mock(Store.class);
        new AddNamedOperationHandler().doOperation(add, context, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler().doOperation(add, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
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
