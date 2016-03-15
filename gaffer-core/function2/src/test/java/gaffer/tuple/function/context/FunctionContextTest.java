package gaffer.tuple.function.context;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import gaffer.function2.mock.MockTransform;
import gaffer.tuple.MapTuple;
import gaffer.tuple.view.TupleView;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotSame;

public class FunctionContextTest {
    private static final ObjectMapper MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }

    protected String serialise(Object object) throws IOException {
        return MAPPER.writeValueAsString(object);
    }

    protected <T> T deserialise(String json, Class<T> type) throws IOException {
        return MAPPER.readValue(json, type);
    }

    @Test
    public void canSelectAndProject() {
        String outputValue = "O";
        String inputValue = "I";
        FunctionContext<MockTransform, String> context = new FunctionContext<>();
        MockTransform mock = new MockTransform(outputValue);
        context.setFunction(mock);
        context.setSelectionView(new TupleView<String>().addHandler("a"));
        context.setProjectionView(new TupleView<String>().addHandler("b", "c"));

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", inputValue);

        context.project(tuple, context.getFunction().transform(context.select(tuple)));

        assertEquals("Unexpected value at reference a", inputValue, tuple.get("a"));
        assertEquals("Unexpected value at reference b", inputValue, tuple.get("b"));
        assertEquals("Unexpected value at reference c", outputValue, tuple.get("c"));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        MockFunctionContext context = new MockFunctionContext();
        MockTransform mock = new MockTransform("a");
        String[] selection = new String[]{"a"};
        String[] projection = new String[]{"b", "c"};

        context.setFunction(mock);
        context.setSelectionView(new TupleView<String>().addHandler(selection[0]));
        context.setProjectionView(new TupleView<String>().addHandler(projection[0]).addHandler(projection[1]));

        String json = serialise(context);
        MockFunctionContext deserialisedContext = deserialise(json, MockFunctionContext.class);

        // check deserialisation
        assertNotNull(deserialisedContext);
        List<List<String>> deserialisedSelection = deserialisedContext.getSelection();
        List<List<String>> deserialisedProjection = deserialisedContext.getProjection();
        assertNotNull(deserialisedContext.getFunction());
        assertNotSame(mock, deserialisedContext.getFunction());
        assertNotSame(context, deserialisedContext);
        assertNotSame(selection, deserialisedSelection);
        assertNotSame(projection, deserialisedProjection);
        assertEquals(selection.length, deserialisedSelection.size());
        assertEquals(selection[0], deserialisedSelection.get(0).get(0));
        assertEquals(projection.length, deserialisedProjection.size());
        assertEquals(projection[0], deserialisedProjection.get(0).get(0));
        assertEquals(projection[1], deserialisedProjection.get(1).get(0));
    }
}
