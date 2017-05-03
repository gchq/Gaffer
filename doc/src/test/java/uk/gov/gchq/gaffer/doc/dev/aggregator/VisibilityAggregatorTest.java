/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.doc.dev.aggregator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VisibilityAggregatorTest {

    @Test
    public void testException() {
        VisibilityAggregator a = new VisibilityAggregator();
        try {
            String state = "public";
            a.apply("public", state);
            a.apply("public", state);
            a.apply("private", state);
            a.apply("public", state);
            a.apply("blah", state);
            a.apply("public", state);
            fail();
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().equals("Visibility must either be 'public' or 'private'. You supplied blah"));
        }
    }

    @Test
    public void testPrivate() {
        VisibilityAggregator a = new VisibilityAggregator();
        String state = "public";
        state = a.apply("public", state);
        state = a.apply("public", state);
        state = a.apply("private", state);
        state = a.apply("public", state);
        state = a.apply("public", state);
        assertEquals("private", state);
    }
}
