/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

public final class GafferResultCacheUtil {
    public static final long ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    public static final long DEFAULT_TIME_TO_LIVE = ONE_DAY_IN_MILLISECONDS;
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheUtil.class);

    private GafferResultCacheUtil() {
    }

    public static Graph createGraph(final String cacheStorePropertiesPath, final Long timeToLive) {
        if (null == cacheStorePropertiesPath) {
            throw new IllegalArgumentException("Gaffer result cache Store properties are required");
        }

        final Graph.Builder graphBuilder = new Graph.Builder()
                .storeProperties(cacheStorePropertiesPath)
                .addSchema(createSchema(timeToLive));

        final Graph graph = graphBuilder.build();
        if (!graph.hasTrait(StoreTrait.STORE_VALIDATION)) {
            LOGGER.warn("Gaffer JSON export graph does not have " + StoreTrait.STORE_VALIDATION.name() + " trait so results may not be aged off.");
        }

        return graph;
    }

    public static Schema createSchema(final Long timeToLive) {
        final Schema.Builder builder = new Schema.Builder()
                .json(StreamUtil.openStreams(GafferResultCacheUtil.class, "gafferResultCache/schema"));

        if (null != timeToLive) {
            builder.merge(new Schema.Builder()
                    .type("timestamp", new TypeDefinition.Builder()
                            .validator(new ElementFilter.Builder()
                                    .execute(new AgeOff(timeToLive))
                                    .build())
                            .build())
                    .build());
        }

        return builder.build();
    }
}
