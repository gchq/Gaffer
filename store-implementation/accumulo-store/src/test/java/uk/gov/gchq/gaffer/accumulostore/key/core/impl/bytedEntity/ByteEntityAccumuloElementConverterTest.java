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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl.bytedEntity;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.AbstractAccumuloElementConverterTest;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Arrays;

/**
 * Tests are inherited from AbstractAccumuloElementConverterTest.
 */
public class ByteEntityAccumuloElementConverterTest extends AbstractAccumuloElementConverterTest {
    @Override
    protected AccumuloElementConverter createConverter(final Schema schema) {
        return new ByteEntityAccumuloElementConverter(schema);
    }

    @Test
    public void shouldSerialiseWithHistoricPropertiesAsBytesFromColumnQualifier() throws Exception {
        // Given
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_3, 3);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_4, 4);
            }
        };

        final byte[] bytes = converter.buildColumnQualifier(TestGroups.EDGE, properties);

        // When
        final byte[] truncatedBytes = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        assertArrayEquals(String.format("\nFound: \n%s\n", Arrays.toString(truncatedBytes)), new byte[]{4, 1, 0, 0, 0, 4, 2, 0, 0, 0}, truncatedBytes);
    }

    @Test
    public void shouldSerialiseWithHistoricColumnQualifier() throws Exception {

        // Given
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_2, Integer.MAX_VALUE);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_3, 3);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_4, Integer.MIN_VALUE);
            }
        };

        // When
        final byte[] columnQualifier = converter.buildColumnQualifier(TestGroups.EDGE, properties);

        // Then
        assertArrayEquals(String.format("\nFound: \n%s\n", Arrays.toString(columnQualifier)), new byte[]{4, 1, 0, 0, 0, 4, -1, -1, -1, 127, 4, 3, 0, 0, 0, 4, 0, 0, 0, -128}, columnQualifier);
    }
}