package gaffer.integration.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import gaffer.commonutil.StreamUtil;
import gaffer.graph.Graph;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;

public class GraphIT {

    public static final String ARRAYLIST_PROPERTIES_PATH = "/arraylist.properties";

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
            new Graph(storePropertiesStream, dataStream, dataTypesStream, storeStream, storeTypesStream);
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
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream dataStream = mock(InputStream.class);
        final InputStream dataTypesStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storeTypesStream = mock(InputStream.class);

        // When
        try {
            new Graph(storePropertiesStream, dataStream, dataTypesStream, storeStream, storeTypesStream);
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
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream dataTypesStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storeTypesStream = mock(InputStream.class);

        // When
        try {
            new Graph(storePropertiesStream, dataStream, dataTypesStream, storeStream, storeTypesStream);
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
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream dataTypesStream = StreamUtil.dataTypes(getClass());
        final InputStream storeStream = StreamUtil.storeSchema(getClass());
        final InputStream storeTypesStream = StreamUtil.storeTypes(getClass());

        // When
        new Graph(storePropertiesStream, dataStream, dataTypesStream, storeStream, storeTypesStream);
        checkClosed(storePropertiesStream);
        checkClosed(dataStream);
        checkClosed(dataTypesStream);
        checkClosed(storeStream);
        checkClosed(storeTypesStream);
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
