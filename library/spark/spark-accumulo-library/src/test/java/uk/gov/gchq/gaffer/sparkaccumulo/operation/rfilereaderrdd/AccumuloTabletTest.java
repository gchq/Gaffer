package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AccumuloTabletTest {

    @Test
    public void testAccumuloTabletEquals() {
        final AccumuloTablet accumuloTablet = new AccumuloTablet(0, 0, "a", "b");

        final AccumuloTablet expected = new AccumuloTablet(0, 0, "a", "b");
        assertTrue(accumuloTablet.equals(expected));
    }
}
