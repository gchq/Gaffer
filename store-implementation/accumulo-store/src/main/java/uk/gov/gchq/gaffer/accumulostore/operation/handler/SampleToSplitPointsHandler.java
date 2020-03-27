package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandler;

public class SampleToSplitPointsHandler extends AbstractSampleToSplitPointsHandler<String, AccumuloStore> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleToSplitPointsHandler.class);

    @Override
    protected Integer getNumSplits(final SampleToSplitPoints operation, final AccumuloStore store) {
        Integer numSplits = super.getNumSplits(operation, store);
        if (null == numSplits) {
            numSplits = getNumAccumuloSplits(store);
        }
        return numSplits;
    }

    private int getNumAccumuloSplits(final AccumuloStore store) {
        int numberTabletServers;
        try {
            numberTabletServers = store.getTabletServers().size();
            LOGGER.debug("Number of region servers is {}", numberTabletServers);
        } catch (final StoreException e) {
            LOGGER.error("Exception thrown getting number of tablet servers: {}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        return numberTabletServers - 1;
    }

}
