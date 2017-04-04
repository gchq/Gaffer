package uk.gov.gchq.koryphe.function;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FunctionMapTest {
    @Test
    public void testMapTransform() {
        int inA = 1;
        int inB = 2;
        int outA = 2;
        int outB = 4;

        Map<String, Integer> inputMap = new HashMap<>();
        inputMap.put("a", inA);
        inputMap.put("b", inB);

        Function<Integer, Integer> functioner = mock(Function.class);
        given(functioner.apply(inA)).willReturn(outA);
        given(functioner.apply(inB)).willReturn(outB);

        FunctionMap<String, Integer, Integer> functionByKey = new FunctionMap<>();
        functionByKey.setFunction(functioner);

        Map<String, Integer> outputMap = functionByKey.apply(inputMap);

        assertEquals(outA, (int) outputMap.get("a"));
        assertEquals(outB, (int) outputMap.get("b"));
    }
}
