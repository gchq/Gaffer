/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleElementsForSplitPointsHandler;

import java.io.IOException;
import java.util.stream.Stream;

public class SampleElementsForSplitPointsHandler extends AbstractSampleElementsForSplitPointsHandler<String, HBaseStore> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleElementsForSplitPointsHandler.class);

    @Override
    protected Stream<String> process(final Stream<? extends Element> stream, final HBaseStore store) {
        final ElementSerialisation serialiser = new ElementSerialisation(store.getSchema());
        return stream
                .map(element -> {
                    try {
                        return serialiser.getRowKeys(element);
                    } catch (final SerialisationException e) {
                        throw new RuntimeException("Unable to serialise element: " + element, e);
                    }
                })
                .flatMap(p -> null == p.getSecond() ? Stream.of(p.getFirst()) : Stream.of(p.getFirst(), p.getSecond()))
                .map(Base64::encodeBase64)
                .map(StringUtil::toString);
    }

    @Override
    protected Integer getNumSplits(final SampleElementsForSplitPoints operation, final HBaseStore store) {
        Integer numSplits = super.getNumSplits(operation, store);
        if (null == numSplits) {
            numSplits = getNumAccumuloSplits(store);
        }
        return numSplits;
    }

    private int getNumAccumuloSplits(final HBaseStore store) {
        int numberRegions;
        try {
            numberRegions = store.getConnection().getAdmin().getTableRegions(store.getTableName()).size();
            LOGGER.debug("Number of regions is {}", numberRegions);
        } catch (final StoreException | IOException e) {
            LOGGER.error("Exception thrown getting number of regions: {}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        return numberRegions - 1;
    }
}
