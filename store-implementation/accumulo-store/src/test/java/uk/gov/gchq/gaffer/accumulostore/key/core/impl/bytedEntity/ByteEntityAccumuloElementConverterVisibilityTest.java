package uk.gov.gchq.gaffer.accumulostore.key.core.impl.bytedEntity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteEntityAccumuloElementConverterVisibilityTest extends ByteEntityAccumuloElementConverterTest {

    @Override
    @BeforeEach
    public void setUp() throws SchemaException, IOException {
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schemaWithVisibilities"));
        converter = createConverter(schema);
    }

    @Override
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
        // An extra 0 at the end of the byte array compared to the parent method accounts for the fact that
        // this schema has one extra property: visibility, which when not set is serialised as EMPTY_BYTES
        byte[] historicColumnQualifierBytes = {4, 1, 0, 0, 0, 4, -1, -1, -1, 127, 4, 3, 0, 0, 0, 4, 0, 0, 0, -128, 0};

        // When
        final byte[] columnQualifier = converter.buildColumnQualifier(TestGroups.EDGE, properties);
        Properties propertiesFromHistoric = converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, historicColumnQualifierBytes);

        // Then
        assertArrayEquals(historicColumnQualifierBytes, columnQualifier);
        // Properties will not set default visibility but when they are
        // made from Accumulo, they will set visibility to "" if not given
        properties.put(AccumuloPropertyNames.VISIBILITY, "");
        assertEquals(propertiesFromHistoric, properties);
    }
}
