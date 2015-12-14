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
 * Unit test of {@link TypeValueRegularExpressionPredicate}. Tests the <code>accept()</code>, <code>equals()</code>,
 * <code>write()</code> and <code>readFields()</code> methods.
 */
public class TestTypeValueRegularExpressionPredicate {

    @Test
    public void testAccept() {
        TypeValueRegularExpressionPredicate predicate = new TypeValueRegularExpressionPredicate(Pattern.compile("A."), Pattern.compile("B."));
        assertTrue(predicate.accept("A1", "B123"));
        assertFalse(predicate.accept("123", "aB"));
        assertFalse(predicate.accept("X", "YS"));
    }

    @Test
    public void testEquals() {
        TypeValueRegularExpressionPredicate predicate1 = new TypeValueRegularExpressionPredicate(Pattern.compile("A."), Pattern.compile("B."));
        TypeValueRegularExpressionPredicate predicate2 = new TypeValueRegularExpressionPredicate(Pattern.compile("A."), Pattern.compile("B."));
        assertEquals(predicate1, predicate2);
        predicate2 = new TypeValueRegularExpressionPredicate(Pattern.compile("A."), Pattern.compile("C."));
        assertNotEquals(predicate1, predicate2);
    }

    @Test
    public void testWriteRead() throws IOException {
        TypeValueRegularExpressionPredicate predicate = new TypeValueRegularExpressionPredicate(Pattern.compile("A."), Pattern.compile("B."));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        TypeValueRegularExpressionPredicate read = new TypeValueRegularExpressionPredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

}
