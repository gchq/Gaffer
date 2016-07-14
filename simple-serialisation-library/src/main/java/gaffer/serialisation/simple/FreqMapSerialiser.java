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
package gaffer.serialisation.simple;

import gaffer.exception.SerialisationException;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.simple.constants.SimpleSerialisationConstants;
import gaffer.types.simple.FreqMap;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;

public class FreqMapSerialiser implements Serialisation  {

    private static final long serialVersionUID = 3772387954385745791L;
    private static final String SEPERATOR = "\\,";
    private static final String SEPERATOR_REGEX = "\\\\,";
    public boolean canHandle(final Class clazz) {
        return FreqMap.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        FreqMap map = (FreqMap) object;
        Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
        StringBuilder builder = new StringBuilder();
        int last = entrySet.size() - 1;
        int start = 0;
        for (Map.Entry<String, Integer> entry : entrySet) {
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
            return builder.toString().getBytes(SimpleSerialisationConstants.ISO_8859_1_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        FreqMap freqMap = new FreqMap();
        if (bytes.length == 0) {
            return freqMap;
        }
        String stringMap;
        try {
            stringMap = new String(bytes, SimpleSerialisationConstants.ISO_8859_1_ENCODING);
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
}
