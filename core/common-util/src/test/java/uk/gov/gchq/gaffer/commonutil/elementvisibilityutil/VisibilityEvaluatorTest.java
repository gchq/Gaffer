/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;

import java.util.regex.PatternSyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility.quote;

/**
 * This test class is copied from org.apache.accumulo.core.security.VisibilityEvaluatorTest.
 */

public class VisibilityEvaluatorTest {

    VisibilityEvaluator ve = new VisibilityEvaluator(new Authorisations("one", "two", "three", "four"));

    @Test
    public void testVisibilityEvaluator() throws VisibilityParseException {
        // test for empty vis
        assertTrue(ve.evaluate(new ElementVisibility(new byte[0])));

        // test for and
        assertTrue("'and' test", ve.evaluate(new ElementVisibility("one&two")));

        // test for or
        assertTrue("'or' test", ve.evaluate(new ElementVisibility("foor|four")));

        // test for and and or
        assertTrue("'and' and 'or' test", ve.evaluate(new ElementVisibility("(one&two)|(foo&bar)")));

        // test for false negatives
        for (String marking : new String[]{"one", "one|five", "five|one", "(one)",
                "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar",
                "(one|foo)|bar", "((one|foo)|bar)&two"}) {
            assertTrue(marking, ve.evaluate(new ElementVisibility(marking)));
        }

        // test for false positives
        for (String marking : new String[]{"five", "one&five",
                "five&one", "((one|foo)|bar)&goober"}) {
            assertFalse(marking, ve.evaluate(new ElementVisibility(marking)));
        }

        // test missing separators; these should throw an exception
        for (String marking : new String[]{"one(five)", "(five)one",
                "(one)(two)", "a|(b(c))"}) {
            try {
                ve.evaluate(new ElementVisibility(marking));
                fail(marking + " failed to throw");
            } catch (PatternSyntaxException e) {
                // all is good
            }
        }

        // test unexpected separator
        for (String marking : new String[]{"&(five)", "|(five)", "(five)&",
                "five|", "a|(b)&", "(&five)", "(five|)"}) {
            try {
                ve.evaluate(new ElementVisibility(marking));
                fail(marking + " failed to throw");
            } catch (PatternSyntaxException e) {
                // all is good
            }
        }

        // test mismatched parentheses
        for (String marking : new String[]{"(", ")", "(a&b", "b|a)"}) {
            try {
                ve.evaluate(new ElementVisibility(marking));
                fail(marking + " failed to throw");
            } catch (PatternSyntaxException e) {
                // all is good
            }
        }

    }

    @Test
    public void testQuotedExpressions() throws VisibilityParseException {
        Authorisations auths = new Authorisations("A#C", "A\"C", "A\\C", "AC");
        VisibilityEvaluator ve = new VisibilityEvaluator(auths);

        assertTrue(ve.evaluate(new ElementVisibility(quote("A#C") + "|" + quote("A?C"))));

        assertFalse(ve.evaluate(new ElementVisibility(quote("A#C") + "&B")));

        assertTrue(ve.evaluate(new ElementVisibility(quote("A#C"))));
        assertTrue(ve.evaluate(new ElementVisibility("(" + quote("A#C") + ")")));
    }

    @Test
    public void testQuote() {
        assertEquals("\"A#C\"", quote("A#C"));
        assertEquals("\"A\\\"C\"", quote("A\"C"));
        assertEquals("\"A\\\"\\\\C\"", quote("A\"\\C"));
        assertEquals("ACS", quote("ACS"));
        assertEquals("\"九\"", quote("九"));
        assertEquals("\"五十\"", quote("五十"));
    }

    @Test
    public void testNonAscii() throws VisibilityParseException {
        VisibilityEvaluator ve = new VisibilityEvaluator(new Authorisations("五", "六", "八", "九", "五十"));

        assertTrue(ve.evaluate(new ElementVisibility(quote("五") + "|" + quote("四"))));
        assertFalse(ve.evaluate(new ElementVisibility(quote("五") + "&" + quote("四"))));
        assertTrue(ve.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")")));
        assertTrue(ve.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"五十\")")));
        assertFalse(ve.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")")));
        assertFalse(ve.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"三\")")));
    }
}
