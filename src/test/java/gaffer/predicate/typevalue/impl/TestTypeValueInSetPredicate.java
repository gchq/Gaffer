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
package gaffer.predicate.typevalue.impl;

import gaffer.Pair;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

/**
 * Unit test of {@link TypeValueInSetPredicate}. Tests the <code>accept()</code>, <code>equals()</code>,
 * <code>write()</code> and <code>readFields()</code> methods.
 */
public class TestTypeValueInSetPredicate {

    @Test
    public void testAccept() {
        TypeValueInSetPredicate predicate = new TypeValueInSetPredicate(new Pair<String>("A", "B"), new Pair<String>("C", "D"));
        assertTrue(predicate.accept("A", "B"));
        assertTrue(predicate.accept("C", "D"));
        assertFalse(predicate.accept("A1", "aB"));
        assertFalse(predicate.accept("X", "YS"));
    }

    @Test
    public void testEquals() {
        TypeValueInSetPredicate predicate1 = new TypeValueInSetPredicate(new Pair<String>("A", "B"), new Pair<String>("C", "D"));
        TypeValueInSetPredicate predicate2 = new TypeValueInSetPredicate(new Pair<String>("A", "B"), new Pair<String>("C", "D"));
        assertEquals(predicate1, predicate2);
        predicate2 = new TypeValueInSetPredicate(new Pair<String>("A", "B"));
        assertNotEquals(predicate1, predicate2);
    }

    @Test
    public void testWriteRead() throws IOException {
        TypeValueInSetPredicate predicate = new TypeValueInSetPredicate(new Pair<String>("A", "B"), new Pair<String>("C", "D"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        TypeValueInSetPredicate read = new TypeValueInSetPredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

}
