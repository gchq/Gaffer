/*
 * Copyright 2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.impl.GenerateSplitPointsFromSample;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractGenerateSplitPointsFromSampleHandler;

public class GenerateSplitPointsFromSampleHandler extends AbstractGenerateSplitPointsFromSampleHandler<String, AccumuloStore> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateSplitPointsFromSampleHandler.class);

    @Override
    protected Integer getNumSplits(final GenerateSplitPointsFromSample operation, final AccumuloStore store) {
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
