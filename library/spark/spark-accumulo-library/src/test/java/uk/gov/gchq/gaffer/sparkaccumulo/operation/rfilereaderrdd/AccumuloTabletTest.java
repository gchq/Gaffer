package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccumuloTabletTest {

    @DisplayName("Equals and hashcode test coverage")
    @Test
    public void testAccumuloTabletEquals() {
        final AccumuloTablet accumuloTablet = new AccumuloTablet(0, 0, "a", "b");

        final AccumuloTablet expected = new AccumuloTablet(0, 0, "a", "b");
        assertTrue(accumuloTablet.equals(expected));
    }
}
