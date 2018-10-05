package uk.gov.gchq.gaffer.types.function;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FreqMapPredicatorTest {

    private FreqMap freqMap;

    @Before
    public void initFreqMap() {
        this.freqMap = new FreqMap();
    }

    @Test
    public void testRegexAndPredicates() {
        Predicate<String> regex = (s) -> s.matches("^\\wo\\w$");
        BiPredicate<String, Long> longPredicate = (s,l) -> l > 1;
        BiPredicate<String, Long> bothPredicate = (s,l) -> regex.test(s) && l > 1;

        FreqMapPredicator fRegexPredicator = new FreqMapPredicator(regex);
        FreqMapPredicator fLongPredicator = new FreqMapPredicator(longPredicate);
        FreqMapPredicator fBothPredicator = new FreqMapPredicator(bothPredicate);

        freqMap.upsert("cat");
        freqMap.upsert("cat");
        freqMap.upsert("dog");
        freqMap.upsert("cow");
        freqMap.upsert("cow");
        freqMap.upsert("catdog");
        freqMap.upsert("catdog");
        freqMap.upsert("catdog");
        freqMap.upsert("cat");
        freqMap.upsert("cat");

        //Check regex works
        FreqMap fRegex = fRegexPredicator.apply(freqMap);

        assertEquals(fRegex.size(), 2);
        assertFalse(fRegex.containsKey("cat"));
        assertTrue(fRegex.containsKey("cow"));

        //Value predicate
        FreqMap fPredicateLong = fLongPredicator.apply(freqMap);

        assertEquals(fPredicateLong.size(), 3);
        assertFalse(fPredicateLong.containsKey("dog"));

        //Both value and key
        FreqMap fPredicateBoth = fBothPredicator.apply(freqMap);

        assertEquals(fPredicateBoth.size(), 1);
        assertFalse(fPredicateBoth.containsKey("dog"));
        assertTrue(fPredicateBoth.containsKey("cow"));

        //Tests to to see the freqMap was not manipulated itself
        assertEquals(freqMap.size(), 4);
        assertTrue(freqMap.containsKey("cat"));
        assertTrue(freqMap.containsKey("dog"));
        assertTrue(freqMap.containsKey("catdog"));
        assertTrue(freqMap.containsKey("cow"));
    }
}
