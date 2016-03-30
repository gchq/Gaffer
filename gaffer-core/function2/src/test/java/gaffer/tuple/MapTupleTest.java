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

package gaffer.tuple;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapTupleTest {
    @Test
    public void testConstructors() {
        MapTuple<String> tuple = new MapTuple<String>();
        int i = 0;
        for (Object value : tuple) {
            i++;
        }
        assertEquals("Unexpected number of values", 0, i);

        int size = 3;
        Map<String, Object> initialMap = new HashMap<>();
        for (i = 0; i < size; i++) {
            initialMap.put("" + i, i);
        }
        tuple = new MapTuple<>(initialMap);
        i = 0;
        for (Object value : tuple) {
            assertEquals("Unexpected value at reference " + value, value, tuple.get("" + value));
            i++;
        }
        assertEquals("Unexpected number of values", size, i);
    }
}
