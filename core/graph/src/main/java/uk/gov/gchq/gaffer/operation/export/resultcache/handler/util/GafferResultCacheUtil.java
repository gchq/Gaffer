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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.function.filter.AgeOff;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

public final class GafferResultCacheUtil {
    public static final long ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    public static final long DEFAULT_TIME_TO_LIVE = ONE_DAY_IN_MILLISECONDS;
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheUtil.class);

    private GafferResultCacheUtil() {
    }

    public static Graph createGraph(final StoreProperties cacheStoreProperties, final Long timeToLive) {
        if (null == cacheStoreProperties) {
            throw new IllegalArgumentException("Store properties have not been set on the initialise export operation");
        }

        final Graph.Builder graphBuilder = new Graph.Builder()
                .storeProperties(cacheStoreProperties)
                .addSchemas(Schema.fromJson(StreamUtil.openStreams(GafferResultCacheUtil.class, "gafferResultCache/schema")));

        if (null != timeToLive) {
            graphBuilder.addSchema(new Schema.Builder()
                    .type("timestamp", new TypeDefinition.Builder()
                            .validator(new ElementFilter.Builder()
                                    .execute(new AgeOff(timeToLive))
                                    .build())
                            .build())
                    .build());
        }

        final Graph graph = graphBuilder.build();
        if (!graph.hasTrait(StoreTrait.STORE_VALIDATION)) {
            LOGGER.warn("Gaffer JSON export graph does not have " + StoreTrait.STORE_VALIDATION.name() + " trait so results may not be aged off.");
        }

        return graph;
    }
}
