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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.MockAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.JobFactory.SCHEMA;

public class AccumuloKeyValueReducerTest {
    @Before
    @After
    public void cleanUp() {
        MockAccumuloElementConverter.cleanUp();
    }

    @Test
    public void shouldGetGroupFromElementConverter() throws IOException, InterruptedException {
        // Given
        MockAccumuloElementConverter.mock = mock(AccumuloElementConverter.class);
        final Key key = mock(Key.class);
        final List<Value> values = Arrays.asList(mock(Value.class), mock(Value.class));
        final Reducer.Context context = mock(Reducer.Context.class);
        final Configuration conf = mock(Configuration.class);
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.ENTITY, new SchemaEdgeDefinition())
                .build();
        final ByteSequence colFamData = mock(ByteSequence.class);
        final byte[] colFam = StringUtil.toBytes(TestGroups.ENTITY);

        given(context.nextKey()).willReturn(true, false);
        given(context.getCurrentKey()).willReturn(key);
        given(context.getValues()).willReturn(values);
        given(context.getConfiguration()).willReturn(conf);
        given(context.getCounter(any(), any())).willReturn(mock(Counter.class));
        given(conf.get(SCHEMA)).willReturn(StringUtil.toString(schema.toCompactJson()));
        given(conf.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS)).willReturn(MockAccumuloElementConverter.class.getName());
        given(colFamData.getBackingArray()).willReturn(colFam);
        given(key.getColumnFamilyData()).willReturn(colFamData);
        given(MockAccumuloElementConverter.mock.getGroupFromColumnFamily(colFam)).willReturn(TestGroups.ENTITY);

        final AccumuloKeyValueReducer reducer = new AccumuloKeyValueReducer();

        // When
        reducer.run(context);

        // Then
        verify(MockAccumuloElementConverter.mock, times(1)).getGroupFromColumnFamily(colFam);
    }
}
