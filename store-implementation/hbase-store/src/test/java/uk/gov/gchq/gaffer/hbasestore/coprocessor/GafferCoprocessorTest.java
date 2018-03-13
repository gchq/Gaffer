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

package uk.gov.gchq.gaffer.hbasestore.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner.QueryScanner;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner.StoreScanner;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GafferCoprocessorTest {
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

    private GafferCoprocessor coprocessor;

    @Before
    public void setup() throws IOException {
        coprocessor = new GafferCoprocessor();
        final CoprocessorEnvironment coEnv = mock(CoprocessorEnvironment.class);
        final Configuration conf = mock(Configuration.class);
        given(coEnv.getConfiguration()).willReturn(conf);
        given(conf.get(HBaseStoreConstants.SCHEMA)).willReturn(StringUtil.escapeComma(Bytes.toString(SCHEMA.toCompactJson())));
        coprocessor.start(coEnv);
    }

    @Test
    public void shouldDelegatePreFlushToStoreScanner() throws IOException {
        // Given
        final ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
        final Store store = mock(Store.class);
        final InternalScanner scanner = mock(InternalScanner.class);

        // When
        final StoreScanner storeScanner = (StoreScanner) coprocessor.preFlush(e, store, scanner);

        // Then
        assertNotNull(storeScanner);
    }

    @Test
    public void shouldDelegatePreCompactWithRequestToStoreScanner() throws IOException {
        // Given
        final ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
        final Store store = mock(Store.class);
        final InternalScanner scanner = mock(InternalScanner.class);
        final CompactionRequest request = mock(CompactionRequest.class);

        // When
        final StoreScanner storeScanner = (StoreScanner) coprocessor.preCompact(e, store, scanner, ScanType.COMPACT_DROP_DELETES, request);

        // Then
        assertNotNull(storeScanner);
    }

    @Test
    public void shouldDelegatePreCompactToStoreScanner() throws IOException {
        // Given
        final ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
        final Store store = mock(Store.class);
        final InternalScanner scanner = mock(InternalScanner.class);

        // When
        final StoreScanner storeScanner = (StoreScanner) coprocessor.preCompact(e, store, scanner, ScanType.COMPACT_DROP_DELETES);

        // Then
        assertNotNull(storeScanner);
    }

    @Test
    public void shouldDelegatePostScannerOpenToQueryScanner() throws IOException {
        // Given
        final ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
        final Scan scan = mock(Scan.class);
        final RegionScanner scanner = mock(RegionScanner.class);

        // When
        final QueryScanner queryScanner = (QueryScanner) coprocessor.postScannerOpen(e, scan, scanner);

        // Then
        assertNotNull(queryScanner);
    }
}
