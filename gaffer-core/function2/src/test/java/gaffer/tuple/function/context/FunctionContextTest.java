package gaffer.tuple.function.context;

import gaffer.function2.StatelessFunction;
import gaffer.tuple.MapTuple;
import gaffer.tuple.tuplen.Tuple2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FunctionContextTest {
    @Test
    public void testBasic() {
        String outputValue = "O";
        String inputValue = "I";
        FunctionContext<MockFunction, String> context = new FunctionContext<>();
        MockFunction mock = new MockFunction(outputValue);
        context.setFunction(mock);
        context.setSelection(new String[]{"a"});
        context.setProjection(new String[]{"b", "c"});

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", inputValue);

        context.project(tuple, context.getFunction().execute(context.select(tuple)));

        assertEquals("Unexpected value at reference a", inputValue, tuple.get("a"));
        assertEquals("Unexpected value at reference b", inputValue, tuple.get("b"));
        assertEquals("Unexpected value at reference c", outputValue, tuple.get("c"));
    }

    private class MockFunction implements StatelessFunction<Object, Tuple2<Object, Object>> {
        private Tuple2<Object, Object> outputTuple;
        private Object output;

        public MockFunction(Object output) {
            outputTuple = Tuple2.createTuple();
            this.output = output;
        }

        @Override
        public Tuple2<Object, Object> execute(Object input) {
            outputTuple.put0(input);
            outputTuple.put1(output);
            return outputTuple;
        }

        @Override
        public MockFunction copy() {
            return new MockFunction(output);
        }
    }
}
