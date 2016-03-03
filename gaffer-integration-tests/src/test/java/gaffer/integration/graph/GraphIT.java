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
        final InputStream dataStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storePropertiesStream = mock(InputStream.class);
        final InputStream typesStream = mock(InputStream.class);

        // When
        try {
            new Graph(dataStream, storeStream, storePropertiesStream, typesStream);
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(dataStream, atLeastOnce()).close();
            verify(storeStream, atLeastOnce()).close();
            verify(storePropertiesStream, atLeastOnce()).close();
            verify(typesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithDataSchema() throws IOException {
        // Given
        final InputStream dataStream = mock(InputStream.class);
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream typesStream = mock(InputStream.class);

        // When
        try {
            new Graph(dataStream, storeStream, storePropertiesStream, typesStream);
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(dataStream, atLeastOnce()).close();
            verify(storeStream, atLeastOnce()).close();
            verify(typesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithDataTypes() throws IOException {
        // Given
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream typesStream = mock(InputStream.class);

        // When
        try {
            new Graph(dataStream, storeStream, storePropertiesStream, typesStream);
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(storeStream, atLeastOnce()).close();
            verify(typesStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithStoreSchema() throws IOException {
        // Given
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream storeStream = mock(InputStream.class);
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream typesStream = StreamUtil.schemaTypes(getClass());

        // When
        try {
            new Graph(dataStream, storeStream, storePropertiesStream, typesStream);
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(storeStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsWhenSuccessful() throws IOException {
        // Given
        final InputStream dataStream = StreamUtil.dataSchema(getClass());
        final InputStream storeStream = StreamUtil.storeSchema(getClass());
        final InputStream storePropertiesStream = StreamUtil.openStream(getClass(), ARRAYLIST_PROPERTIES_PATH);
        final InputStream typesStream = StreamUtil.schemaTypes(getClass());

        // When
        new Graph(dataStream, storeStream, storePropertiesStream, typesStream);
        checkClosed(dataStream);
        checkClosed(storeStream);
        checkClosed(storePropertiesStream);
        checkClosed(typesStream);
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
