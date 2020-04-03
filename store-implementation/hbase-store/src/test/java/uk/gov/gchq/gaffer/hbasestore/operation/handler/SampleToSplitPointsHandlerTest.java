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
package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandlerTest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SampleToSplitPointsHandlerTest extends AbstractSampleToSplitPointsHandlerTest<HBaseStore> {

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
    protected HBaseStore createStore() {
        return store;
    }

    @Override
    protected AbstractSampleToSplitPointsHandler<String, HBaseStore> createHandler() {
        return new SampleToSplitPointsHandler();
    }

    @Test
    public void shouldUseTheNumberOfRegionsToCalculateNumSplits() throws OperationException {

        // Given
        final Integer numSplits = null;
        final List<String> sample = createSampleOfSize(100);

        final SampleToSplitPoints<String> operation = new SampleToSplitPoints.Builder<String>()
                .input(sample)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), store);

        // Then
        final int expectedNumSplits = NUM_TABLE_REGIONS - 1;
        assertEquals(expectedNumSplits, splits.size());
    }
}
