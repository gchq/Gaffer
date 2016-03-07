package gaffer.tuple.tuplen;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TupleNTest {
    @Test
    public void testTupleNFactoryMethods() {
        Tuple1 tuple1 = Tuple1.createTuple();
        int i = 0;
        for (Object values : tuple1) {
            i++;
        }
        assertEquals("Unexpected number of values in tuple1", 1, i);

        Tuple2 tuple2 = Tuple2.createTuple();
        i = 0;
        for (Object values : tuple2) {
            i++;
        }
        assertEquals("Unexpected number of values in tuple2", 2, i);


        Tuple3 tuple3 = Tuple3.createTuple();
        i = 0;
        for (Object values : tuple3) {
            i++;
        }
        assertEquals("Unexpected number of values in tuple3", 3, i);


        Tuple4 tuple4 = Tuple4.createTuple();
        i = 0;
        for (Object values : tuple4) {
            i++;
        }
        assertEquals("Unexpected number of values in tuple4", 4, i);


        Tuple5 tuple5 = Tuple5.createTuple();
        i = 0;
        for (Object values : tuple5) {
            i++;
        }
        assertEquals("Unexpected number of values in tuple5", 5, i);
    }
}
