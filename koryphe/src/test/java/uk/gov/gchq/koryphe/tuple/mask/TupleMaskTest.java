/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.koryphe.tuple.mask;

import uk.gov.gchq.koryphe.tuple.MapTuple;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.n.Tuple1;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.n.Tuple4;
import uk.gov.gchq.koryphe.tuple.n.mask.TupleMask2;
import uk.gov.gchq.koryphe.tuple.n.mask.TupleMask3;
import uk.gov.gchq.koryphe.tuple.n.mask.TupleMaskN;
import org.junit.Test;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;
import uk.gov.gchq.koryphe.tuple.n.Tuple5;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleMaskTest {
    @Test
    public void testOneDimensionalReferences() {
        String a = "a";
        String b = "b";
        String c = "c";

        TupleMask<String, Tuple3<String, String, String>> adapter = new TupleMask3(new TupleMask(a), new TupleMask(b), new TupleMask(c));

        Tuple<String> inputTuple = mock(Tuple.class);
        given(inputTuple.get(a)).willReturn(a);
        given(inputTuple.get(b)).willReturn(b);
        given(inputTuple.get(c)).willReturn(c);

        Tuple3<String, String, String> selected = (Tuple3) adapter.select(inputTuple);

        assertEquals("Unexpected value selected by mask", a, selected.get0());
        assertEquals("Unexpected value selected by mask", b, selected.get1());
        assertEquals("Unexpected value selected by mask", c, selected.get2());

        Tuple<String> resultTuple = mock(Tuple.class);

        List<String> projection = Arrays.asList(a, b, c);
        adapter.setContext(resultTuple);
        adapter.project(projection);

        verify(resultTuple, times(1)).put(a, a);
        verify(resultTuple, times(1)).put(b, b);
        verify(resultTuple, times(1)).put(c, c);
    }

    @Test
    public void testTwoDimensionalReferences() {
        String a1 = "a1";
        String a2 = "a2";
        String b1 = "b1";
        String c1 = "c1";
        String c2 = "c2";
        String c3 = "c3";

        TupleMask<String, Tuple3<Tuple2<String, String>, String, Tuple3<String, String, String>>> adapter = new TupleMask3<>(new TupleMask2(new TupleMask(a1), new TupleMask(a2)), new TupleMask(b1), new TupleMask3(new TupleMask(c1), new TupleMask(c2), new TupleMask(c3)));

        Tuple<String> inputTuple = mock(Tuple.class);
        given(inputTuple.get(a1)).willReturn(a1);
        given(inputTuple.get(a2)).willReturn(a2);
        given(inputTuple.get(b1)).willReturn(b1);
        given(inputTuple.get(c1)).willReturn(c1);
        given(inputTuple.get(c2)).willReturn(c2);
        given(inputTuple.get(c3)).willReturn(c3);

        Tuple3<Tuple2<String, String>, String, Tuple3<String, String, String>> selected = (Tuple3) adapter.select(inputTuple);
        Tuple2<String, String> a = selected.get0();
        String b = selected.get1();
        Tuple3<String, String, String> c = selected.get2();

        assertEquals("Unexpected value selected by mask", a1, a.get0());
        assertEquals("Unexpected value selected by mask", a2, a.get1());
        assertEquals("Unexpected value selected by mask", b1, b);
        assertEquals("Unexpected value selected by mask", c1, c.get0());
        assertEquals("Unexpected value selected by mask", c2, c.get1());
        assertEquals("Unexpected value selected by mask", c3, c.get2());

        Tuple<String> resultTuple = mock(Tuple.class);

        adapter.setContext(resultTuple);
        List<Object> projection = Arrays.asList(Arrays.asList(a1, a2), b1, Arrays.asList(c1, c2, c3));
        adapter.project(projection);

        verify(resultTuple, times(1)).put(a1, a1);
        verify(resultTuple, times(1)).put(a2, a2);
        verify(resultTuple, times(1)).put(b1, b1);
        verify(resultTuple, times(1)).put(c1, c1);
        verify(resultTuple, times(1)).put(c2, c2);
        verify(resultTuple, times(1)).put(c3, c3);
    }

    @Test
    public void testTupleNAccessors() {
        String a = "a";
        Integer b = 1;
        Long c = 2l;
        Double d = 3.0d;
        Float e = 4.0f;

        MapTuple<String> tuple = new MapTuple<>();
        TupleMask<String, Tuple5<String, Integer, Long, Double, Float>> adapter = new TupleMaskN(new TupleMask("a"), new TupleMask("b"), new TupleMask("c"),
                                                new TupleMask("d"), new TupleMask("e"));
        adapter.setContext(tuple);

        Tuple1<String> tuple1 = (Tuple1) adapter;
        tuple1.put0(a);
        assertEquals("Unexpected value at index 0", a, tuple1.get0());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));

        Tuple2<String, Integer> tuple2 = (Tuple2) adapter;
        tuple2.put1(b);
        assertEquals("Unexpected value at index 0", a, tuple2.get0());
        assertEquals("Unexpected value at index 1", b, tuple2.get1());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));

        Tuple3<String, Integer, Long> tuple3 = (Tuple3) adapter;
        tuple3.put2(c);
        assertEquals("Unexpected value at index 0", a, tuple3.get0());
        assertEquals("Unexpected value at index 1", b, tuple3.get1());
        assertEquals("Unexpected value at index 2", c, tuple3.get2());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));
        assertEquals("Unexpected value at reference c", c, tuple.get("c"));

        Tuple4<String, Integer, Long, Double> tuple4 = (Tuple4) adapter;
        tuple4.put3(d);
        assertEquals("Unexpected value at index 0", a, tuple4.get0());
        assertEquals("Unexpected value at index 1", b, tuple4.get1());
        assertEquals("Unexpected value at index 2", c, tuple4.get2());
        assertEquals("Unexpected value at index 3", d, tuple4.get3());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));
        assertEquals("Unexpected value at reference c", c, tuple.get("c"));
        assertEquals("Unexpected value at reference d", d, tuple.get("d"));

        Tuple5<String, Integer, Long, Double, Float> tuple5 = (Tuple5) adapter;
        tuple5.put4(e);
        assertEquals("Unexpected value at index 0", a, tuple5.get0());
        assertEquals("Unexpected value at index 1", b, tuple5.get1());
        assertEquals("Unexpected value at index 2", c, tuple5.get2());
        assertEquals("Unexpected value at index 3", d, tuple5.get3());
        assertEquals("Unexpected value at index 4", e, tuple5.get4());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));
        assertEquals("Unexpected value at reference c", c, tuple.get("c"));
        assertEquals("Unexpected value at reference d", d, tuple.get("d"));
        assertEquals("Unexpected value at reference e", e, tuple.get("e"));
    }
}
