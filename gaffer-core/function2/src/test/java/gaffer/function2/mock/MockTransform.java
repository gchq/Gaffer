package gaffer.function2.mock;

import gaffer.function2.Transformer;
import gaffer.tuple.tuplen.Tuple2;

public class MockTransform extends Transformer<Object, Tuple2<Object, Object>> {
    private Tuple2<Object, Object> outputTuple;
    private Object output;

    public MockTransform() {}

    public MockTransform(Object output) {
        outputTuple = Tuple2.createTuple();
        this.output = output;
    }

    public void setOutput(Object output) {
        this.output = output;
    }

    public Object getOutput() {
        return output;
    }

    @Override
    public Tuple2<Object, Object> transform(Object input) {
        outputTuple.put0(input);
        outputTuple.put1(output);
        return outputTuple;
    }

    @Override
    public MockTransform copy() {
        return new MockTransform(output);
    }
}
