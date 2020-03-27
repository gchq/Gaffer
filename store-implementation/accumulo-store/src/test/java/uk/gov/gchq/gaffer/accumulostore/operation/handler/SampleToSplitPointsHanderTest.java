package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.junit.Before;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandlerTest;

import java.util.Collections;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SampleToSplitPointsHanderTest extends AbstractSampleToSplitPointsHandlerTest<AccumuloStore> {

    public static final int NUM_TABLET_SERVERS = 4;

    private AccumuloStore store;

    @Before
    public void before() throws StoreException {
        store = mock(AccumuloStore.class);
        final AccumuloKeyPackage keyPackage = mock(AccumuloKeyPackage.class);
        final AccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(schema);
        final List<String> tabletServers = Collections.nCopies(NUM_TABLET_SERVERS, null);

        given(store.getKeyPackage()).willReturn(keyPackage);
        given(keyPackage.getKeyConverter()).willReturn(converter);
        given(store.getTabletServers()).willReturn(tabletServers);
    }

    @Override
    protected AccumuloStore createStore() {
        return store;
    }

    @Override
    protected AbstractSampleToSplitPointsHandler<String, AccumuloStore> createHandler() {
        return new SampleToSplitPointsHandler();
    }
}
