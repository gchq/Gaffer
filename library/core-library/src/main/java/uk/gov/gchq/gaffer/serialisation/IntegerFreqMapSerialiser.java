/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.serialisation;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.IntegerFreqMap;
import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @deprecated use {@link IntegerFreqMap} with {@link FreqMapSerialiser} instead.
 */
@Deprecated
public class IntegerFreqMapSerialiser implements Serialisation<IntegerFreqMap> {

    private static final long serialVersionUID = 3772387954385745791L;
    private static final String SEPERATOR = "\\,";
    private static final String SEPERATOR_REGEX = "\\\\,";

    public boolean canHandle(final Class clazz) {
        return IntegerFreqMap.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final IntegerFreqMap map) throws SerialisationException {
        Set<Entry<String, Integer>> entrySet = map.entrySet();
        StringBuilder builder = new StringBuilder();
        int last = entrySet.size() - 1;
        int start = 0;
        for (final Entry<String, Integer> entry : entrySet) {
            Integer value = entry.getValue();
            if (value == null) {
                continue;
            }
            builder.append(entry.getKey() + SEPERATOR + value);
            ++start;
            if (start > last) {
                break;
            }
            builder.append(SEPERATOR);
        }
        try {
            return builder.toString()
                          .getBytes(CommonConstants.ISO_8859_1_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public IntegerFreqMap deserialise(final byte[] bytes) throws SerialisationException {
        IntegerFreqMap freqMap = new IntegerFreqMap();
        if (bytes.length == 0) {
            return freqMap;
        }
        String stringMap;
        try {
            stringMap = new String(bytes, CommonConstants.ISO_8859_1_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        if (stringMap.isEmpty()) {
            //No values so return the empty map
            return freqMap;
        }
        String[] keyValues = stringMap.split(SEPERATOR_REGEX);
        if (keyValues.length % 2 != 0) {
            throw new SerialisationException("Uneven number of entries found for serialised frequency map, unable to deserialise.");
        }
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            freqMap.put(keyValues[i], Integer.parseInt(keyValues[i + 1]));
        }
        return freqMap;
    }

    @Override
    public IntegerFreqMap deserialiseEmptyBytes() throws SerialisationException {
        return new IntegerFreqMap();
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
