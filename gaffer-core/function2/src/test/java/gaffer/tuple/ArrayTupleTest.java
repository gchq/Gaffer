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
        String put0 = "a";
        Integer put1 = 1;
        Long put2 = 2l;
        Double put3 = 3.0d;
        Float put4 = 4.0f;

        ArrayTuple tuple = new ArrayTuple(5);

        Tuple1<String> tuple1 = (Tuple1) tuple;
        tuple1.put0(put0);
        String got0 = tuple1.get0();
        assertEquals("Unexpected value at index 0", put0, got0);

        Tuple2<String, Integer> tuple2 = (Tuple2) tuple;
        tuple2.put1(put1);
        got0 = tuple2.get0();
        Integer got1 = tuple2.get1();
        assertEquals("Unexpected value at index 0", put0, got0);
        assertEquals("Unexpected value at index 1", put1, got1);

        Tuple3<String, Integer, Long> tuple3 = (Tuple3) tuple;
        tuple3.put2(put2);
        got0 = tuple3.get0();
        got1 = tuple3.get1();
        Long got2 = tuple3.get2();
        assertEquals("Unexpected value at index 0", put0, got0);
        assertEquals("Unexpected value at index 1", put1, got1);
        assertEquals("Unexpected value at index 2", put2, got2);

        Tuple4<String, Integer, Long, Double> tuple4 = (Tuple4) tuple;
        tuple4.put3(put3);
        got0 = tuple4.get0();
        got1 = tuple4.get1();
        got2 = tuple4.get2();
        Double got3 = tuple4.get3();
        assertEquals("Unexpected value at index 0", put0, got0);
        assertEquals("Unexpected value at index 1", put1, got1);
        assertEquals("Unexpected value at index 2", put2, got2);
        assertEquals("Unexpected value at index 3", put3, got3);

        Tuple5<String, Integer, Long, Double, Float> tuple5 = (Tuple5) tuple;
        tuple5.put4(put4);
        got0 = tuple5.get0();
        got1 = tuple5.get1();
        got2 = tuple5.get2();
        got3 = tuple5.get3();
        Float got4 = tuple5.get4();
        assertEquals("Unexpected value at index 0", put0, got0);
        assertEquals("Unexpected value at index 1", put1, got1);
        assertEquals("Unexpected value at index 2", put2, got2);
        assertEquals("Unexpected value at index 3", put3, got3);
        assertEquals("Unexpected value at index 4", put4, got4);
    }
}
