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

package uk.gov.gchq.gaffer.graph;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.operation.export.resultcache.GafferResultCacheExporter;
import uk.gov.gchq.gaffer.operation.export.resultcache.handler.util.GafferResultCacheUtil;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertTrue;

public class GafferResultExportHandlerTest {
    @Test
    public void shouldHaveAValidSchema() {
        // When
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(GafferResultCacheExporter.class, "gafferResultCache/schema"));
        assertTrue(schema.validate());

        final Edge edge = new Edge.Builder()
                .group("result")
                .source("jobId")
                .dest("exportId")
                .directed(true)
                .property("timestamp", System.currentTimeMillis())
                .property("deletionTimestamp", System.currentTimeMillis() + GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE)
                .property("visibility", "")
                .property("result", "test".getBytes())
                .build();

        assertTrue(new ElementValidator(schema).validate(edge));
        assertTrue(schema.validate());
    }
}