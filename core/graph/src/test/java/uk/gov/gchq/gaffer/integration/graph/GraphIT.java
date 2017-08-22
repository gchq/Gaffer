package uk.gov.gchq.gaffer.integration.graph;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
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
        final InputStream elementsSchemaStream = mock(InputStream.class);
        final InputStream typesSchemaStream = mock(InputStream.class);
        final InputStream aggregationSchemaStream = mock(InputStream.class);
        final InputStream validationSchemaStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .storeProperties(storePropertiesStream)
                    .addSchema(elementsSchemaStream)
                    .addSchema(typesSchemaStream)
                    .addSchema(aggregationSchemaStream)
                    .addSchema(validationSchemaStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(storePropertiesStream, atLeastOnce()).close();
            verify(elementsSchemaStream, atLeastOnce()).close();
            verify(typesSchemaStream, atLeastOnce()).close();
            verify(aggregationSchemaStream, atLeastOnce()).close();
            verify(validationSchemaStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithElementSchema() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream elementSchemaStream = mock(InputStream.class);
        final InputStream typesSchemaStream = mock(InputStream.class);
        final InputStream serialisationSchemaStream = mock(InputStream.class);
        final InputStream aggregationSchemaStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("graph1")
                            .build())
                    .storeProperties(storePropertiesStream)
                    .addSchema(elementSchemaStream)
                    .addSchema(typesSchemaStream)
                    .addSchema(serialisationSchemaStream)
                    .addSchema(aggregationSchemaStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(elementSchemaStream, atLeastOnce()).close();
            verify(typesSchemaStream, atLeastOnce()).close();
            verify(serialisationSchemaStream, atLeastOnce()).close();
            verify(aggregationSchemaStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsIfExceptionThrownWithTypesSchema() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream elementSchemaStream = StreamUtil.elementsSchema(getClass());
        final InputStream typesSchemaStream = mock(InputStream.class);
        final InputStream aggregationSchemaStream = mock(InputStream.class);
        final InputStream serialisationSchemaStream = mock(InputStream.class);

        // When
        try {
            new Graph.Builder()
                    .storeProperties(storePropertiesStream)
                    .addSchema(elementSchemaStream)
                    .addSchema(typesSchemaStream)
                    .addSchema(aggregationSchemaStream)
                    .addSchema(serialisationSchemaStream)
                    .build();
            fail("Exception expected");
        } catch (final Exception e) {
            // Then
            assertNotNull(e.getMessage());
            verify(typesSchemaStream, atLeastOnce()).close();
            verify(aggregationSchemaStream, atLeastOnce()).close();
            verify(serialisationSchemaStream, atLeastOnce()).close();
        }
    }

    @Test
    public void shouldCloseStreamsWhenSuccessful() throws IOException {
        // Given
        final InputStream storePropertiesStream = StreamUtil.storeProps(getClass());
        final InputStream elementsSchemaStream = StreamUtil.elementsSchema(getClass());
        final InputStream typesSchemaStream = StreamUtil.typesSchema(getClass());

        // When
        new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .storeProperties(storePropertiesStream)
                .addSchema(elementsSchemaStream)
                .addSchema(typesSchemaStream)
                .build();
        checkClosed(storePropertiesStream);
        checkClosed(elementsSchemaStream);
        checkClosed(typesSchemaStream);
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
