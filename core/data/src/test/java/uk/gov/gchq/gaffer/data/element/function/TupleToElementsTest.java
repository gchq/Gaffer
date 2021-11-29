package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.MapTuple;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TupleToElementsTest extends FunctionTest<TupleToElements> {

    @Test
    void shouldConvertBasicEntity() {
        // Given
        ElementTupleDefinition elementTupleDefinition = new ElementTupleDefinition(TestGroups.ENTITY);
        elementTupleDefinition.vertex("vertex");
        final TupleToElements tupleToElements = new TupleToElements();
        List<ElementTupleDefinition> elements = Stream.of(elementTupleDefinition).collect(Collectors.toList());
        tupleToElements.elements(elements);
        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("vertex", "a");

        // When
        Iterable<Element> results = tupleToElements.apply(tuple);

        // Then
        List<Element> expected = new ArrayList<>();
        expected.add(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("a")
                .build());
        assertThat(results).containsExactlyElementsOf(expected);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Iterable.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final TupleToElements tupleToElements = new TupleToElements();
        // When
        final String json = new String(JSONSerialiser.serialise(tupleToElements));
        TupleToElements deserialisedTupleToElements = JSONSerialiser.deserialise(json, TupleToElements.class);
        // Then
        assertEquals(tupleToElements, deserialisedTupleToElements);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.data.element.function.TupleToElements\"}", json);

    }

    @Override
    protected TupleToElements getInstance() {
        return new TupleToElements();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}