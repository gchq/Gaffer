package uk.gov.gchq.gaffer.integration.graph;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GraphIT {
    @Test
    public void shouldCloseStreamsIfExceptionThrownWithStoreProperties() throws IOException {
        // Given
        final InputStream storePropertiesStream = mock(InputStream.class);
        final InputStream dataStream = mock(InputStream.class);
        final InputStream dataTypesStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storeTypesStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .storeProperties(storePropertiesStream)
                    .addSchema(dataStream)
                    .addSchema(dataTypesStream)
                    .addSchema(storeStream)
                    .addSchema(storeTypesStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(storePropertiesStream, atLeastOnce()).close();
            verify(dataStream, atLeastOnce()).close();
            verify(dataTypesStream, atLeastOnce()).close();
            verify(storeStream, atLeastOnce()).close();
            verify(storeTypesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithDataSchema() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream dataStream = mock(InputStream.class);
        final InputStream dataTypesStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storeTypesStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .storeProperties(storePropertiesStream)
                    .addSchema(dataStream)
                    .addSchema(dataTypesStream)
                    .addSchema(storeStream)
                    .addSchema(storeTypesStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(dataStream, atLeastOnce()).close();
            verify(dataTypesStream, atLeastOnce()).close();
            verify(storeStream, atLeastOnce()).close();
            verify(storeTypesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithDataTypes() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream dataTypesStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storeTypesStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .storeProperties(storePropertiesStream)
                    .addSchema(dataStream)
                    .addSchema(dataTypesStream)
                    .addSchema(storeStream)
                    .addSchema(storeTypesStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(dataTypesStream, atLeastOnce()).close();
            verify(storeStream, atLeastOnce()).close();
            verify(storeTypesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsWhenSuccessful() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream dataTypesStream = StreamUtil.dataTypes(getClass());

        // When
        new Graph.Builder()
                .storeProperties(storePropertiesStream)
                .addSchema(dataStream)
                .addSchema(dataTypesStream)
                .build();
        checkClosed(storePropertiesStream);
        checkClosed(dataStream);
        checkClosed(dataTypesStream);
    }

    private void checkClosed(final InputStream stream) {
        try {
            int result = stream.read();
            fail("Exception expected");
        } catch (final IOException e) {
            assertEquals("Stream closed", e.getMessage());
        }
    }
}
