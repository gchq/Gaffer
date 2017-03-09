package koryphe.function.aggregate;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class AggregateByKeyTest {
    @Test
    public void testMapAggregation() {
        int inA = 1;
        int inB = 2;

        int noInputs = 3;

        Map<String, Integer>[] inputs = new HashMap[noInputs];
        for (int i = 0; i < noInputs; i++) {
            inputs[i] = new HashMap<String, Integer>();
            inputs[i].put("a", inA);
            inputs[i].put("b", inB);
        }

        // create mock that adds input and state together
        Aggregator<Integer> aggregator = mock(Aggregator.class);
        for (int i = 0; i < noInputs; i++) {
            Integer expectedA = null;
            Integer expectedB = null;
            if (i > 0) {
                expectedA = i * inA;
                expectedB = i * inB;
            }
            given(aggregator.execute(inA, expectedA)).willReturn(inA + (expectedA == null ? 0 : expectedA));
            given(aggregator.execute(inB, expectedB)).willReturn(inB + (expectedB == null ? 0 : expectedB));
        }

        AggregateByKey<String, Integer> mapAggregator = new AggregateByKey<>();
        mapAggregator.setFunction(aggregator);

        Map<String, Integer> state = null;
        for (Map<String, Integer> input : inputs) {
            state = mapAggregator.execute(input, state);
        }

        assertEquals(noInputs * inA, (int) state.get("a"));
        assertEquals(noInputs * inB, (int) state.get("b"));
    }
}
