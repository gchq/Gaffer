/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner;

import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.ElementDedupeFilterProcessor;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.GafferScannerProcessor;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.StoreAggregationProcessor;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.ValidationProcessor;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class StoreScannerTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .build())
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final Schema SCHEMA_NO_AGGREGATION = new Schema.Builder()
            .type("string", String.class)
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .aggregate(false)
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .aggregate(false)
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final View VIEW = new View.Builder()
            .entity(TestGroups.ENTITY_2)
            .edge(TestGroups.EDGE_2)
            .build();

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldConstructProcessors() throws OperationException, IOException {
        // Given
        final Scan scan = mock(Scan.class);
        given(scan.getAttribute(HBaseStoreConstants.VIEW)).willReturn(VIEW.toCompactJson());
        given(scan.getAttribute(HBaseStoreConstants.EXTRA_PROCESSORS)).willReturn(StringUtil.toCsv(ElementDedupeFilterProcessor.class));

        // When
        final List<GafferScannerProcessor> processors = StoreScanner.createProcessors(SCHEMA, serialisation);

        // Then
        assertEquals(2, processors.size());
        int i = 0;
        assertTrue(processors.get(i) instanceof StoreAggregationProcessor);
        assertEquals(SCHEMA, ((StoreAggregationProcessor) processors.get(i)).getSchema());

        i++;
        assertTrue(processors.get(i) instanceof ValidationProcessor);
        assertEquals(SCHEMA, ((ValidationProcessor) processors.get(i)).getSchema());
    }

    @Test
    public void shouldConstructProcessorsWithNoAggregation() throws OperationException, IOException {
        // Given
        final Scan scan = mock(Scan.class);
        given(scan.getAttribute(HBaseStoreConstants.VIEW)).willReturn(VIEW.toCompactJson());
        given(scan.getAttribute(HBaseStoreConstants.EXTRA_PROCESSORS)).willReturn(StringUtil.toCsv(ElementDedupeFilterProcessor.class));

        // When
        final List<GafferScannerProcessor> processors = StoreScanner.createProcessors(SCHEMA_NO_AGGREGATION, serialisation);

        // Then
        assertEquals(1, processors.size());
        int i = 0;
        assertTrue(processors.get(i) instanceof ValidationProcessor);
        assertEquals(SCHEMA_NO_AGGREGATION, ((ValidationProcessor) processors.get(i)).getSchema());
    }
}
