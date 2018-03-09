/*
 * Copyright 2016-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyAccumuloElementConverterTest;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.ByteUtils;
import uk.gov.gchq.gaffer.accumulostore.utils.BytesAndRange;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests are inherited from AbstractAccumuloElementConverterTest.
 */
public class ByteEntityAccumuloElementConverterTest extends AbstractCoreKeyAccumuloElementConverterTest {
    @Override
    protected ByteEntityAccumuloElementConverter createConverter(final Schema schema) {
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
        byte[] historicPropertyBytes = {4, 1, 0, 0, 0, 4, 2, 0, 0, 0};
        final byte[] columnQualifierBytes = converter.buildColumnQualifier(TestGroups.EDGE, properties);

        // When
        final BytesAndRange propertiesBytes = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, columnQualifierBytes, 2);

        // Then
        assertTrue(ByteUtils.areKeyBytesEqual(new BytesAndRange(historicPropertyBytes, 0, historicPropertyBytes.length), propertiesBytes));
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
        byte[] historicColumnQualifierBytes = {4, 1, 0, 0, 0, 4, -1, -1, -1, 127, 4, 3, 0, 0, 0, 4, 0, 0, 0, -128};

        // When
        final byte[] columnQualifier = converter.buildColumnQualifier(TestGroups.EDGE, properties);
        Properties propertiesFromHistoric = converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, historicColumnQualifierBytes);

        // Then
        assertArrayEquals(historicColumnQualifierBytes, columnQualifier);
        assertEquals(propertiesFromHistoric, properties);
    }


}
