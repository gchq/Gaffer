package gaffer.tuple.handler;

import gaffer.tuple.MapTuple;
import gaffer.tuple.tuplen.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TupleViewTest {
    @Test
    public void testOneDimensionalReferences() {
        String a = "a";
        String b = "b";
        String c = "c";

        TupleView<String> view = new TupleView<>(new String[]{a, b, c});

        MapTuple<String> inputTuple = new MapTuple<>();
        inputTuple.put(a, a);
        inputTuple.put(b, b);
        inputTuple.put(c, c);

        Tuple3<String, String, String> selected = (Tuple3)view.select(inputTuple);

        assertEquals("Unexpected value selected by view", a, selected.get0());
        assertEquals("Unexpected value selected by view", b, selected.get1());
        assertEquals("Unexpected value selected by view", c, selected.get2());

        MapTuple<String> resultTuple = new MapTuple<>();

        List<String> projection = Arrays.asList(a, b, c);
        view.project(resultTuple, projection);

        assertEquals("Unexpected value projected by view", a, resultTuple.get(a));
        assertEquals("Unexpected value projected by view", b, resultTuple.get(b));
        assertEquals("Unexpected value projected by view", c, resultTuple.get(c));
    }

    @Test
    public void testTwoDimensionalReferences() {
        String a1 = "a1";
        String a2 = "a2";
        String b1 = "b1";
        String c1 = "c1";
        String c2 = "c2";
        String c3 = "c3";

        TupleView<String> view = new TupleView<>(new String[][]{{a1, a2}, {b1}, {c1, c2, c3}});

        MapTuple<String> inputTuple = new MapTuple<>();
        inputTuple.put(a1, a1);
        inputTuple.put(a2, a2);
        inputTuple.put(b1, b1);
        inputTuple.put(c1, c1);
        inputTuple.put(c2, c2);
        inputTuple.put(c3, c3);

        Tuple3<Tuple2<String, String>, String, Tuple3<String, String, String>> selected = (Tuple3)view.select(inputTuple);
        Tuple2<String, String> a = selected.get0();
        String b = selected.get1();
        Tuple3<String, String, String> c = selected.get2();

        assertEquals("Unexpected value selected by view", a1, a.get0());
        assertEquals("Unexpected value selected by view", a2, a.get1());
        assertEquals("Unexpected value selected by view", b1, b);
        assertEquals("Unexpected value selected by view", c1, c.get0());
        assertEquals("Unexpected value selected by view", c2, c.get1());
        assertEquals("Unexpected value selected by view", c3, c.get2());

        MapTuple<String> resultTuple = new MapTuple<>();

        List<Object> projection = Arrays.asList(Arrays.asList(a1, a2), b1, Arrays.asList(c1, c2, c3));
        view.project(resultTuple, projection);

        assertEquals("Unexpected value projected by view", a1, resultTuple.get(a1));
        assertEquals("Unexpected value projected by view", a2, resultTuple.get(a2));
        assertEquals("Unexpected value projected by view", b1, resultTuple.get(b1));
        assertEquals("Unexpected value projected by view", c1, resultTuple.get(c1));
        assertEquals("Unexpected value projected by view", c2, resultTuple.get(c2));
        assertEquals("Unexpected value projected by view", c3, resultTuple.get(c3));
    }

    @Test
    public void testTupleNAccessors() {
        String a = "a";
        Integer b = 1;
        Long c = 2l;
        Double d = 3.0d;
        Float e = 4.0f;

        MapTuple<String> tuple = new MapTuple<>();
        TupleView view = new TupleView(new String[]{"a", "b", "c", "d", "e"});
        view.setTuple(tuple);

        Tuple1<String> tuple1 = (Tuple1)view;
        tuple1.put0(a);
        assertEquals("Unexpected value at index 0", a, tuple1.get0());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));

        Tuple2<String, Integer> tuple2 = (Tuple2)view;
        tuple2.put1(b);
        assertEquals("Unexpected value at index 0", a, tuple2.get0());
        assertEquals("Unexpected value at index 1", b, tuple2.get1());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));

        Tuple3<String, Integer, Long> tuple3 = (Tuple3)view;
        tuple3.put2(c);
        assertEquals("Unexpected value at index 0", a, tuple3.get0());
        assertEquals("Unexpected value at index 1", b, tuple3.get1());
        assertEquals("Unexpected value at index 2", c, tuple3.get2());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));
        assertEquals("Unexpected value at reference c", c, tuple.get("c"));

        Tuple4<String, Integer, Long, Double> tuple4 = (Tuple4)view;
        tuple4.put3(d);
        assertEquals("Unexpected value at index 0", a, tuple4.get0());
        assertEquals("Unexpected value at index 1", b, tuple4.get1());
        assertEquals("Unexpected value at index 2", c, tuple4.get2());
        assertEquals("Unexpected value at index 3", d, tuple4.get3());
        assertEquals("Unexpected value at reference a", a, tuple.get("a"));
        assertEquals("Unexpected value at reference b", b, tuple.get("b"));
        assertEquals("Unexpected value at reference c", c, tuple.get("c"));
        assertEquals("Unexpected value at reference d", d, tuple.get("d"));

        Tuple5<String, Integer, Long, Double, Float> tuple5 = (Tuple5)view;
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
