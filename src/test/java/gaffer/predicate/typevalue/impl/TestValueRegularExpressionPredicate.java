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

import org.junit.Test;

import java.io.*;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Unit test of {@link ValueRegularExpressionPredicate}. Tests the <code>accept()</code>, <code>equals()</code>,
 * <code>write()</code> and <code>readFields()</code> methods.
 */
public class TestValueRegularExpressionPredicate {

    @Test
    public void testAccept() {
        ValueRegularExpressionPredicate predicate = new ValueRegularExpressionPredicate(Pattern.compile("A."));
        assertTrue(predicate.accept("123", "A1"));
        assertFalse(predicate.accept("A1", "aB"));
        assertFalse(predicate.accept("X", "YS"));
    }

    @Test
    public void testEquals() {
        ValueRegularExpressionPredicate predicate1 = new ValueRegularExpressionPredicate(Pattern.compile("A."));
        ValueRegularExpressionPredicate predicate2 = new ValueRegularExpressionPredicate(Pattern.compile("A."));
        assertEquals(predicate1, predicate2);
        predicate2 = new ValueRegularExpressionPredicate(Pattern.compile("B."));
        assertNotEquals(predicate1, predicate2);
    }

    @Test
    public void testWriteRead() throws IOException {
        ValueRegularExpressionPredicate predicate = new ValueRegularExpressionPredicate(Pattern.compile("A."));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        ValueRegularExpressionPredicate read = new ValueRegularExpressionPredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

}
