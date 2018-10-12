/*
 * Copyright 2017-2018. Crown Copyright
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

/**
 * This class is used to sort the seeds to optimise the way the {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}
 * maps a seed to a specific file which allows the seed filter to only be applied to the relevant files.
 */
public class SeedComparator implements Comparator<Object>, Serializable {

    private static final long serialVersionUID = 8415485366776438127L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SeedComparator.class);

    public SeedComparator() {
        super();
    }

    @Override
    public int compare(final Object obj1, final Object obj2) {
        if (null == obj1 && null == obj2) {
            throw new IllegalArgumentException("Cannot call compare on SeedsComparator with both arguments null");
        }
        if (null == obj1) {
            // Obj2 is != null and is therefore > obj1
            return -1;
        } else if (null == obj2) {
            // Obj1 is != null and is therefore < obj2
            return 1;
        }
        if (obj1 instanceof Object[] && obj2 instanceof Object[]) {
            Object[] o1 = (Object[]) obj1;
            Object[] o2 = (Object[]) obj2;

            if (o1.length == o2.length) {
                return sameLengthComparison(o1, o2);
            }
            if (o1.length < o2.length) {
                return diffLengthComparison(o1, o2);
            }
            return -diffLengthComparison(o2, o1);
        }
        LOGGER.error("Expected to get Object[]'s but found: {} and {}",
                obj1.getClass(), obj2.getClass());
        return Integer.MAX_VALUE;
    }

    private int diffLengthComparison(final Object[] o1, final Object[] o2) {
        if (o1.length >= o2.length) {
            throw new IllegalArgumentException("o1 should be shorter than o2");
        }
        for (int i = 0; i < o1.length; i++) {
            Object innerObject1 = o1[i];
            Object innerObject2 = o2[i];
            if (innerObject1 instanceof Comparable && innerObject2.getClass().equals(innerObject1.getClass())) {
                int result = ((Comparable) innerObject1).compareTo(innerObject2);
                if (result != 0) {
                    return result;
                }
            } else if (innerObject1 instanceof byte[] && innerObject2 instanceof byte[]) {
                int result = UnsignedBytes.lexicographicalComparator().compare((byte[]) innerObject1, (byte[]) innerObject2);
                if (result != 0) {
                    return result;
                }
            } else {
                LOGGER.error("Trying to compare objects of type {} and {}. You need to be comparing objects of the same type, check that the seeds are of the same type as the vertices.",
                        o1.getClass(), o2.getClass());
                return Integer.MAX_VALUE;
            }
        }
        return 0;
    }

    private int sameLengthComparison(final Object[] o1, final Object[] o2) {
        for (int i = 0; i < o1.length && i < o2.length; i++) {
            Object innerObject1 = o1[i];
            Object innerObject2 = o2[i];
            if (innerObject1 instanceof Comparable && innerObject2.getClass().equals(innerObject1.getClass())) {
                int result = ((Comparable) innerObject1).compareTo(innerObject2);
                if (result != 0) {
                    return result;
                }
            } else if (innerObject1 instanceof byte[] && innerObject2 instanceof byte[]) {
                int result = UnsignedBytes.lexicographicalComparator().compare((byte[]) innerObject1, (byte[]) innerObject2);
                if (result != 0) {
                    return result;
                }
            } else {
                LOGGER.error("Trying to compare objects of type {} and {}. You need to be comparing objects of the same type, check that the seeds are of the same type as the vertices.",
                        o1.getClass(), o2.getClass());
                return Integer.MAX_VALUE;
            }
        }
        return 0;
    }
}
