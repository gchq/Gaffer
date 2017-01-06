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

package uk.gov.gchq.gaffer.example.gettingstarted.function.aggregate;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VisibilityAggregatorTest {

    @Test
    public void testException() {
        VisibilityAggregator a = new VisibilityAggregator();
        try {
            a.aggregate(new Object[]{"public"});
            a.aggregate(new Object[]{"public"});
            a.aggregate(new Object[]{"private"});
            a.aggregate(new Object[]{"public"});
            a.aggregate(new Object[]{"blah"});
            a.aggregate(new Object[]{"public"});
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().equals("Visibility must either be 'public' or 'private'. You supplied blah"));
        }
    }

    @Test
    public void testPrivate() {
        VisibilityAggregator a = new VisibilityAggregator();
        a.aggregate(new Object[]{"public"});
        a.aggregate(new Object[]{"public"});
        a.aggregate(new Object[]{"private"});
        a.aggregate(new Object[]{"public"});
        a.aggregate(new Object[]{"public"});
        assertTrue(a.state()[0].toString().equals("private"));
    }
}
