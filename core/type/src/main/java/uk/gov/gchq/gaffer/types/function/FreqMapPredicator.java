package uk.gov.gchq.gaffer.types.function;

import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * A {@code FreqMapExtractor} is a {@link KorypheFunction} that extracts a
 * count from a frequency map for the provided key.
 */
@Since("1.7.0")
@Summary("Returns a frequency map based on the predicate provided")
public class FreqMapPredicator extends KorypheFunction<FreqMap, FreqMap> {

    private BiPredicate<String, Long> predicate;

    public FreqMapPredicator(Predicate<String> predicate) {
        this.predicate = (s, aLong) -> predicate.test(s);
    }

    public FreqMapPredicator(BiPredicate<String, Long> predicate) {
        this.predicate = predicate;
    }

    /**
     * Creates a filtered copy of the map using a supplied predicate.
     *
     * @param map  The frequency map that is to be sorted through
     * @return  A new frequency map with only the filtered entries present.
     */
    private FreqMap filterPredicate(FreqMap map) {
        FreqMap f = new FreqMap();

        map.entrySet().stream().filter(e -> predicate.test(e.getKey(), e.getValue()))
                .forEach(e -> f.upsert(e.getKey(), e.getValue()));

        return f;
    }

    @Override
    public FreqMap apply(FreqMap freqMap) {
        return filterPredicate(freqMap);
    }
}
