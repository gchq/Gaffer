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
 * Unit test of {@link TypeRegularExpressionPredicate}. Tests the <code>accept()</code>, <code>equals()</code>,
 * <code>write()</code> and <code>readFields()</code> methods.
 */
public class TestTypeRegularExpressionPredicate {

    @Test
    public void testAccept() {
        TypeRegularExpressionPredicate predicate = new TypeRegularExpressionPredicate(Pattern.compile("A."));
        assertTrue(predicate.accept("A1", "123"));
        assertFalse(predicate.accept("123", "aB"));
        assertFalse(predicate.accept("X", "YS"));
    }

    @Test
    public void testEquals() {
        TypeRegularExpressionPredicate predicate1 = new TypeRegularExpressionPredicate(Pattern.compile("A."));
        TypeRegularExpressionPredicate predicate2 = new TypeRegularExpressionPredicate(Pattern.compile("A."));
        assertEquals(predicate1, predicate2);
        predicate2 = new TypeRegularExpressionPredicate(Pattern.compile("B."));
        assertNotEquals(predicate1, predicate2);
    }

    @Test
    public void testWriteRead() throws IOException {
        TypeRegularExpressionPredicate predicate = new TypeRegularExpressionPredicate(Pattern.compile("A."));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        TypeRegularExpressionPredicate read = new TypeRegularExpressionPredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

}
