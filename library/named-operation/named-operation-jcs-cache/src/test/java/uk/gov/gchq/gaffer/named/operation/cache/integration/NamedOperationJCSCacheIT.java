package uk.gov.gchq.gaffer.named.operation.cache.integration;


import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.named.operation.*;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.NamedOperationJCSCache;
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

import static org.junit.Assert.assertEquals;

public class NamedOperationJCSCacheIT {

    private static Graph graph;

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

        final StoreProperties storeProps = StoreProperties.loadStoreProperties(StreamUtil.storeProps(NamedOperationJCSCacheIT.class));
        Store store = Class.forName(storeProps.getStoreClass()).asSubclass(Store.class).newInstance();
        store.initialise(new Schema(), storeProps);
        graph = new Graph.Builder()
                .store(store)
                .build();
    }

    @After
    public void after() throws CacheOperationFailedException {
        new NamedOperationJCSCache().clear();
    }

    @Test
    public void shouldBeAbleToAddNamedOperationToCache() throws OperationException {

        // given
        GetAllNamedOperations get = new GetAllNamedOperations.Builder().build();

        // when
        graph.execute(add, user);

        NamedOperationDetail expectedNamedOp  = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .description(add.getDescription())
                .operationChain(add.getOperationChain())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .build();

        List<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        List<NamedOperationDetail> results = Lists.newArrayList(graph.execute(get, user));

        // then
        assertEquals(1, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
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

    @Test
    public void shouldAllowUpdatingOfNamedOperations() throws OperationException {
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
