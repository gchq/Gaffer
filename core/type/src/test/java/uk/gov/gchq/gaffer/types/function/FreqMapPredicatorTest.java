package uk.gov.gchq.gaffer.types.function;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.types.FreqMap;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class FreqMapPredicatorTest {

    private FreqMap freqMap;

    @Before
    public void initFreqMap() {
        this.freqMap = new FreqMap();

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
    }

    @Test
    public void shouldFilterKeysUsingRegexOnKey() {
        //given
        Predicate<String> regex = (s) -> s.matches("^\\wo\\w$");
        FreqMapPredicator fRegexPredicator = new FreqMapPredicator(regex);

        //when
        FreqMap fRegex = fRegexPredicator.apply(freqMap);

        //then
        assertEquals(fRegex.size(), 2);
        assertFalse(fRegex.containsKey("cat"));
        assertTrue(fRegex.containsKey("cow"));
    }

    @Test
    public void shouldFilterBasedOnValue() {
        //given
        BiPredicate<String, Long> longPredicate = (s, l) -> l > 1;
        FreqMapPredicator fLongPredicator = new FreqMapPredicator(longPredicate);

        //when
        FreqMap fPredicateLong = fLongPredicator.apply(freqMap);

        //then
        assertEquals(fPredicateLong.size(), 3);
        assertFalse(fPredicateLong.containsKey("dog"));
    }

    @Test
    public void shouldFilterBasedOnKeyAndValue() {
        //given
        BiPredicate<String, Long> bothPredicate = (s, l) -> s.matches("^\\wo\\w$") && l > 1;
        FreqMapPredicator fBothPredicator = new FreqMapPredicator(bothPredicate);

        //when
        FreqMap fPredicateBoth = fBothPredicator.apply(freqMap);

        //then
        assertEquals(fPredicateBoth.size(), 1);
        assertFalse(fPredicateBoth.containsKey("dog"));
        assertTrue(fPredicateBoth.containsKey("cow"));
    }

    @Test
    public void shouldHandleNulls() {
        //given
        Predicate<String> nullRegex = null;
        BiPredicate<String, Long> nullBiPredicate = null;

        //when
        FreqMapPredicator nullRegPredicator = new FreqMapPredicator(nullRegex);
        FreqMapPredicator nullBiPredicator = new FreqMapPredicator(nullBiPredicate);

        //then
        assertNull(nullRegPredicator.apply(freqMap));
        assertNull(nullBiPredicator.apply(freqMap));
    }

    @Test
    public void shouldNotMutateOriginalValue() {
        //given
        Predicate<String> regex = (s) -> s.matches("^\\wo\\w$");
        FreqMapPredicator fRegexPredicator = new FreqMapPredicator(regex);

        //when
        // (this should result in the new map being size 2).
        fRegexPredicator.apply(freqMap);

        //then
        assertEquals(freqMap.size(), 4);
        assertTrue(freqMap.containsKey("cat"));
        assertTrue(freqMap.containsKey("dog"));
        assertTrue(freqMap.containsKey("catdog"));
        assertTrue(freqMap.containsKey("cow"));
    }
}
