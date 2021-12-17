package uk.gov.gchq.gaffer.types.function;

import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.Iterator;
import java.util.List;


/**
 * CountsToFreqMap is a KorypheFunction which takes an array of counts and converts them
 * to a FreqMap according to a predefined array of Keys. For this to work, the length of
 * the two arrays must match. If either the keys or input data is null, an empty frequency
 * map will be returned.
 */
@Since("1.14.1")
@Summary("Converts an iterable to a Frequency Map")
public class CountsToFreqMap extends KorypheFunction<List<Long>, FreqMap> {
    private List<String> freqMapKeys;

    public CountsToFreqMap() {
        // For Json Serialisation;
    }

    public CountsToFreqMap(List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
    }

    @Override
    public FreqMap apply(List<Long> counts) {
        FreqMap freqMap = new FreqMap();

        if (this.freqMapKeys == null || counts == null) {
            return freqMap;
        }
        Iterator<String> keysIterator = freqMapKeys.iterator();
        Iterator<Long> valuesIterator = counts.iterator();

        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            freqMap.upsert(keysIterator.next(), valuesIterator.next());
        }

        return freqMap;
    }

    public Iterable<String> getFreqMapKeys() {
        return freqMapKeys;
    }

    public void setFreqMapKeys(List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
    }

    public CountsToFreqMap freqMapKeys(List<String> freqMapKeys) {
        this.freqMapKeys = freqMapKeys;
        return this;
    }
}
