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

package uk.gov.gchq.gaffer.graph.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.nio.file.Paths;

public class InitialiseGafferJsonExportHandler implements OperationHandler<InitialiseGafferJsonExport, Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InitialiseGafferJsonExportHandler.class);

    /**
     * Time to live in milliseconds - cannot be more than the default time to live
     */
    private Long timeToLive = InitialiseGafferJsonExport.DEFAULT_TIME_TO_LIVE;

    private StoreProperties storeProperties;

    @Override
    public Object doOperation(final InitialiseGafferJsonExport operation,
                              final Context context, final Store store)
            throws OperationException {
        final GafferJsonExporter exporter = operation.getExporter();
        exporter.initialise(createGraph(), operation, store, context.getUser(), context.getExecutionId());
        context.addExporter(exporter);
        return operation.getInput();
    }

    private Graph createGraph() {
        if (null == storeProperties) {
            throw new IllegalArgumentException("Store properties have not been set on the initialise export operation");
        }

        final Graph graph = new Graph.Builder()
                .storeProperties(storeProperties)
                .addSchemas(StreamUtil.openStreams(getClass(), "gafferJsonExport/schema"))
                .build();

        if (!graph.hasTrait(StoreTrait.STORE_VALIDATION)) {
            LOGGER.warn("Gaffer JSON export graph does not have " + StoreTrait.STORE_VALIDATION.name() + " trait so results may not be aged off.");
        }

        return graph;
    }

    public Long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(final Long timeToLive) {
        if (null != timeToLive && timeToLive > InitialiseGafferJsonExport.DEFAULT_TIME_TO_LIVE) {
            throw new IllegalArgumentException("Time to live must be less than or equal to: " + InitialiseGafferJsonExport.DEFAULT_TIME_TO_LIVE);
        }
        this.timeToLive = timeToLive;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public void setStoreProperties(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }


    public void setStorePropertiesPath(final String path) {
        setStoreProperties(StoreProperties.loadStoreProperties(Paths.get(path)));
    }
}
