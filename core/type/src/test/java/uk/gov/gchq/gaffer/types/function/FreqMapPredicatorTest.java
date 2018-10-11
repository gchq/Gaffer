package uk.gov.gchq.gaffer.types.function;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.impl.predicate.Regex;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
    public void shouldFilterMapWithMultipleResults() {
        //given
        final Regex predicate = new Regex("^\\wo\\w$");
        final FreqMapPredicator fRegexPredicator = new FreqMapPredicator(predicate);

        //when
        final FreqMap fRegex = fRegexPredicator.apply(freqMap);

        //then
        assertEquals(fRegex.size(), 2);
        assertTrue(fRegex.containsKey("cow"));
        assertTrue(fRegex.containsKey("dog"));
    }

    @Test
    public void shouldFilterMapWithSingleResult() {
        //given
        final Regex predicate = new Regex("^c.*o.*g$");
        final FreqMapPredicator fRegexPredicator = new FreqMapPredicator(predicate);

        //when
        final FreqMap fRegex = fRegexPredicator.apply(freqMap);

        //then
        assertEquals(fRegex.size(), 1);
        assertTrue(fRegex.containsKey("catdog"));
    }

    @Test
    public void shouldHandleNulls() {
        //given
        final FreqMapPredicator nullRegPredicator = new FreqMapPredicator(null);

        //when
        final FreqMap map = nullRegPredicator.apply(freqMap);

        //then
        assertThat(map, is(freqMap));
    }

    @Test
    public void shouldNotMutateOriginalValue() {
        //given
        final Regex predicate = new Regex("^\\wo\\w$");
        final FreqMapPredicator fRegexPredicator = new FreqMapPredicator(predicate);

        //when
        final FreqMap fRegex = fRegexPredicator.apply(freqMap);

        //then
        assertEquals(fRegex.size(), 2);
        assertTrue(fRegex.containsKey("cow"));
        assertTrue(fRegex.containsKey("dog"));

        assertEquals(freqMap.size(), 4);
        assertTrue(freqMap.containsKey("cat"));
        assertTrue(freqMap.containsKey("dog"));
        assertTrue(freqMap.containsKey("catdog"));
        assertTrue(freqMap.containsKey("cow"));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        //given
        final FreqMapPredicator nullPredicator = new FreqMapPredicator();
        final FreqMapPredicator regexPredicator = new FreqMapPredicator(new Regex("^\\wo\\w$"));

        //when
        final String json = new String(JSONSerialiser.serialise(nullPredicator, true));
        final String json2 = new String(JSONSerialiser.serialise(regexPredicator, false));

        //then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.FreqMapPredicator\"%n" +
                "}"), json);

        JsonAssert.assertEquals("{\"class\":\"uk.gov.gchq.gaffer.types.function.FreqMapPredicator\"," +
                "\"predicate\":{\"class\":\"uk.gov.gchq.koryphe.impl.predicate.Regex\",\"value\":" +
                "{\"java.util.regex.Pattern\":\"^\\\\wo\\\\w$\"}}}", json2);

        final FreqMapPredicator deserializedNull = JSONSerialiser.deserialise(json, FreqMapPredicator.class);
        final FreqMapPredicator deserializedRegex = JSONSerialiser.deserialise(json2, FreqMapPredicator.class);

        assertEquals(deserializedNull.apply(freqMap).size(), freqMap.size());
        assertEquals(deserializedRegex.apply(freqMap).size(), 2);
    }
}
