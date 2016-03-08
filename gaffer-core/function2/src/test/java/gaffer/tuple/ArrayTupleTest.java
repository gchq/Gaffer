package gaffer.tuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import gaffer.tuple.tuplen.*;
import org.junit.Test;

public class ArrayTupleTest {
    @Test
    public void testConstructors() {
        // test size constructor
        int size = 3;
        ArrayTuple tuple = new ArrayTuple(size);
        int i = 0;
        for (Object value : tuple) {
            i++;
            if (value != null) fail("Found unexpected non-null value");
        }
        assertEquals("Found unexpected number of values", size, i);


        // test initial array constructor
        String[] initialValues = new String[]{"a", "b", "c", "d", "e"};
        tuple = new ArrayTuple(initialValues);
        i = 0;
        for (Object value : tuple) {
            assertEquals("Found unexpected tuple value", value, initialValues[i]);
            i ++;
        }
        assertEquals("Found unexpected number of values", initialValues.length, i);
    }

    @Test
    public void testTupleNAccessors() {
        String a = "a";
        Integer b = 1;
        Long c = 2l;
        Double d = 3.0d;
        Float e = 4.0f;

        ArrayTuple tuple = new ArrayTuple(5);

        Tuple1<String> tuple1 = (Tuple1)tuple;
        tuple1.put0(a);
        assertEquals("Unexpected value at index 0", a, tuple1.get0());
        assertEquals("Unexpected value at reference 0", a, tuple.get(0));

        Tuple2<String, Integer> tuple2 = (Tuple2)tuple;
        tuple2.put1(b);
        assertEquals("Unexpected value at index 0", a, tuple2.get0());
        assertEquals("Unexpected value at index 1", b, tuple2.get1());
        assertEquals("Unexpected value at reference 0", a, tuple.get(0));
        assertEquals("Unexpected value at reference 1", b, tuple.get(1));

        Tuple3<String, Integer, Long> tuple3 = (Tuple3)tuple;
        tuple3.put2(c);
        assertEquals("Unexpected value at index 0", a, tuple3.get0());
        assertEquals("Unexpected value at index 1", b, tuple3.get1());
        assertEquals("Unexpected value at index 2", c, tuple3.get2());
        assertEquals("Unexpected value at reference 0", a, tuple.get(0));
        assertEquals("Unexpected value at reference 1", b, tuple.get(1));
        assertEquals("Unexpected value at reference 2", c, tuple.get(2));

        Tuple4<String, Integer, Long, Double> tuple4 = (Tuple4)tuple;
        tuple4.put3(d);
        assertEquals("Unexpected value at index 0", a, tuple4.get0());
        assertEquals("Unexpected value at index 1", b, tuple4.get1());
        assertEquals("Unexpected value at index 2", c, tuple4.get2());
        assertEquals("Unexpected value at index 3", d, tuple4.get3());
        assertEquals("Unexpected value at reference 0", a, tuple.get(0));
        assertEquals("Unexpected value at reference 1", b, tuple.get(1));
        assertEquals("Unexpected value at reference 2", c, tuple.get(2));
        assertEquals("Unexpected value at reference 3", d, tuple.get(3));

        Tuple5<String, Integer, Long, Double, Float> tuple5 = (Tuple5) tuple;
        tuple5.put4(e);
        assertEquals("Unexpected value at index 0", a, tuple5.get0());
        assertEquals("Unexpected value at index 1", b, tuple5.get1());
        assertEquals("Unexpected value at index 2", c, tuple5.get2());
        assertEquals("Unexpected value at index 3", d, tuple5.get3());
        assertEquals("Unexpected value at index 4", e, tuple5.get4());
        assertEquals("Unexpected value at reference 0", a, tuple.get(0));
        assertEquals("Unexpected value at reference 1", b, tuple.get(1));
        assertEquals("Unexpected value at reference 2", c, tuple.get(2));
        assertEquals("Unexpected value at reference 3", d, tuple.get(3));
        assertEquals("Unexpected value at reference 4", e, tuple.get(4));
    }
}
