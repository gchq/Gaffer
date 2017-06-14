/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;

public class SeedComparator implements Comparator<Object>, Serializable {

    private static final long serialVersionUID = 8415485366776438127L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SeedComparator.class);

    public SeedComparator() {
        super();
    }

    @Override
    public int compare(final Object obj1, final Object obj2) {
        if (obj1 instanceof Object[] && obj2 instanceof Object[]) {
            Object[] o1 = (Object[]) obj1;
            Object[] o2 = (Object[]) obj2;
            for (int i = 0; i < o1.length; i++) {
                if (o1[i] instanceof Comparable && o2[i].getClass().equals(o1[i].getClass())) {
                    int result = ((Comparable) o1[i]).compareTo((Comparable) o2[i]);
                    if (!(result == 0)) {
                        return result;
                    }
                } else if (o1[i] instanceof byte[] && o2[i] instanceof byte[]) {
                    int result = UnsignedBytes.lexicographicalComparator().compare((byte[]) o1[i], (byte[]) o2[i]);
                    if (!(result == 0)) {
                        return result;
                    }
                } else {
                    LOGGER.error("Trying to compare objects of type " + o1.getClass() + " and " + o2.getClass() +
                            ". You need to be comparing objects of the same type, check that the seeds are of the same type as the vertices.");
                    return Integer.MAX_VALUE;
                }
            }
            return 0;
        }
        LOGGER.error("Expected to get Object[]'s but found: " + obj1.getClass() + " and " + obj2.getClass());
        return Integer.MAX_VALUE;
    }
}
