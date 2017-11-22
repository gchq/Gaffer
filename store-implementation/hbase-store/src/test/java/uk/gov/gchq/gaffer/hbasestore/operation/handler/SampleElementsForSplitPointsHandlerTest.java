/*
 * Copyright 2017 Crown Copyright
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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleElementsForSplitPointsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleElementsForSplitPointsHandlerTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SampleElementsForSplitPointsHandlerTest extends AbstractSampleElementsForSplitPointsHandlerTest<HBaseStore> {
    public static final int NUM_TABLE_REGIONS = 4;
    private HBaseStore store;

    @Before
    public void before() throws StoreException, IOException {
        store = mock(HBaseStore.class);
        final Connection connection = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final TableName tableName = TableName.valueOf("table1");
        final List<HRegionInfo> tableRegions = Collections.nCopies(NUM_TABLE_REGIONS, null);

        given(store.getSchema()).willReturn(schema);
        given(store.getConnection()).willReturn(connection);
        given(store.getTableName()).willReturn(tableName);
        given(connection.getAdmin()).willReturn(admin);
        given(admin.getTableRegions(tableName)).willReturn(tableRegions);
    }

    @Override
    public void shouldThrowExceptionIfNumSplitsIsNull() {
        // Num splits can never be null
    }

    @Test
    public void shouldUseTheNumberOfRegionsToCalculateNumSplits() throws OperationException {
        // Given
        final Integer numSplits = null;
        final List<Element> elements = IntStream.range(0, 30)
                .mapToObj(i -> new Edge(TestGroups.EDGE, "source_" + i, "dest_" + i, true))
                .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<String, HBaseStore> handler = createHandler();
        final SampleElementsForSplitPoints<String> operation = new SampleElementsForSplitPoints.Builder<String>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(NUM_TABLE_REGIONS, splits.size() + 1);
        verifySplits(Arrays.asList(14, 29, 44), elements, splits, handler);
    }

    @Test
    public void shouldCalculateRequiredNumberOfSplitsFromEdges() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements = IntStream.range(0, numSplits * 10)
                .mapToObj(i -> new Edge(TestGroups.EDGE, "source_" + i, "dest_" + i, true))
                .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<String, HBaseStore> handler = createHandler();
        final SampleElementsForSplitPoints<String> operation = new SampleElementsForSplitPoints.Builder<String>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), createStore());

        // Then
        verifySplits(Arrays.asList(14, 29, 44), elements, splits, handler);
    }

    @Override
    protected HBaseStore createStore() {
        return store;
    }

    @Override
    protected AbstractSampleElementsForSplitPointsHandler<String, HBaseStore> createHandler() {
        return new SampleElementsForSplitPointsHandler();
    }
}
