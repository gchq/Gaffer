/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;

import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility.quote;

/**
 * This test class is copied from org.apache.accumulo.core.security.VisibilityEvaluatorTest.
 */

public class VisibilityEvaluatorTest {

    final VisibilityEvaluator ve = new VisibilityEvaluator(new Authorisations("one", "two", "three", "four"));

    @Test
    public void testVisibilityEvaluator() throws VisibilityParseException {
        // test for empty vis
        assertTrue(ve.evaluate(new ElementVisibility(new byte[0])));

        // test for and
        assertTrue(ve.evaluate(new ElementVisibility("one&two")), "'and' test");

        // test for or
        assertTrue(ve.evaluate(new ElementVisibility("foor|four")), "'or' test");

        // test for and and or
        assertTrue(ve.evaluate(new ElementVisibility("(one&two)|(foo&bar)")), "'and' and 'or' test");
    }

    @ParameterizedTest
    @ValueSource(strings = {"one", "one|five", "five|one", "(one)", "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar",
            "(one|foo)|bar", "((one|foo)|bar)&two"})
    public void testFalseNegatives(String marking) throws VisibilityParseException {
        assertTrue(ve.evaluate(new ElementVisibility(marking)), marking);
    }

    @ParameterizedTest
    @ValueSource(strings = {"five", "one&five", "five&one", "((one|foo)|bar)&goober"})
    public void testFalsePositives(String marking) throws VisibilityParseException {
        assertFalse(ve.evaluate(new ElementVisibility(marking)), marking);
    }

    @ParameterizedTest
    @ValueSource(strings = {"one(five)", "(five)one", "(one)(two)", "a|(b(c))"})
    public void testMissingSeparatorsShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @ParameterizedTest
    @ValueSource(strings = {"&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)"})
    public void testUnexpectedSeparatorShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @ParameterizedTest
    @ValueSource(strings = {"(", ")", "(a&b", "b|a)"})
    public void testMismatchedParenthesisShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @Test
    public void testQuotedExpressions() throws VisibilityParseException {
        final Authorisations auths = new Authorisations("A#C", "A\"C", "A\\C", "AC");
        final VisibilityEvaluator ve = new VisibilityEvaluator(auths);

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
        final VisibilityEvaluator ve = new VisibilityEvaluator(new Authorisations("五", "六", "八", "九", "五十"));

        assertTrue(ve.evaluate(new ElementVisibility(quote("五") + "|" + quote("四"))));
        assertFalse(ve.evaluate(new ElementVisibility(quote("五") + "&" + quote("四"))));
        assertTrue(ve.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")")));
        assertTrue(ve.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"五十\")")));
        assertFalse(ve.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")")));
        assertFalse(ve.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"三\")")));
    }
}
