package uk.gov.gchq.gaffer.store.serialiser;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EdgeSerialiserTest {
    private EdgeSerialiser edgeSerialiser;
    private Schema schema;

    @Before
    public void setUp() {

        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_2, TestTypes.PROP_STRING)
                .build();

        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .edge(TestGroups.EDGE, edgeDef)
                .build();
        edgeSerialiser = new EdgeSerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder()
                .build();

        // When / Then
        try {
            edgeSerialiser = new EdgeSerialiser(schema);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Vertex serialiser is required"));
        }
    }

    /*@Test
    public void testInvalidSerialiser() {
        // Given
        schema = new Schema.Builder()
                .vertexSerialiser()
                .build();

        // When / Then
        try {
            edgeSerialiser = new EdgeSerialiser(schema);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Vertex serialiser must be a"));
        }
    }*/

    @Test
    public void testCanSeraliseEdge() throws SerialisationException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);

        // When
        final byte[] serialisedEdge = edgeSerialiser.serialise(edge);
        final Edge deserialisedEdge = edgeSerialiser.deserialise(serialisedEdge);

        // Then
        assertEquals(edge, deserialisedEdge);
    }

    @Test
    public void cantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(edgeSerialiser.canHandle(Integer.class));
    }

    @Test
    public void canSerialiseEdgeClass() throws SerialisationException {
        assertTrue(edgeSerialiser.canHandle(Edge.class));
    }
}
