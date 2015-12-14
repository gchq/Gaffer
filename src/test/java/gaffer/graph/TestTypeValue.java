/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.graph;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the <code>compareTo</code> method on {@link TypeValue}.
 */
public class TestTypeValue {

    @Test
    public void testCompare() {
        TypeValue se1 = new TypeValue("customer", "A");
        TypeValue se2 = new TypeValue("customer", "B");
        TypeValue se3 = new TypeValue("customerX", "A");
        assertTrue(se1.compareTo(se2) < 0);
        assertTrue(se1.compareTo(se3) < 0);
        assertTrue(se2.compareTo(se3) < 0);
        assertEquals(0, se1.compareTo(se1));
    }

}
