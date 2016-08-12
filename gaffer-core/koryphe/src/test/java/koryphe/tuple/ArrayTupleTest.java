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

package koryphe.tuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import koryphe.tuple.ArrayTuple;
import org.junit.Test;

public class ArrayTupleTest {
    @Test
    public void testConstructors() {
        // test size constructor
        int size = 3;
        ArrayTuple tuple = new ArrayTuple(size);
        int i = 0;
        for (Object value : tuple) {
            i++;
            if (value != null) fail("Found unexpected non-null value");
        }
        assertEquals("Found unexpected number of values", size, i);


        // test initial array constructor
        String[] initialValues = new String[]{"a", "b", "c", "d", "e"};
        tuple = new ArrayTuple(initialValues);
        i = 0;
        for (Object value : tuple) {
            assertEquals("Found unexpected tuple value", value, initialValues[i]);
            i ++;
        }
        assertEquals("Found unexpected number of values", initialValues.length, i);
    }
}
