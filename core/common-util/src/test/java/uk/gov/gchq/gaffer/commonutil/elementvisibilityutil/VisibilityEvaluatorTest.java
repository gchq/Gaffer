/*
 * Copyright 2017-2024 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility.quote;

/**
 * This test class is copied from org.apache.accumulo.core.security.VisibilityEvaluatorTest.
 */

class VisibilityEvaluatorTest {

    final VisibilityEvaluator ve = new VisibilityEvaluator(new Authorisations("one", "two", "three", "four"));

    @Test
    void testVisibilityEvaluator() throws VisibilityParseException {
        // test for empty vis
        assertThat(ve.evaluate(new ElementVisibility(new byte[0]))).isTrue();

        // test for and
        assertThat(ve.evaluate(new ElementVisibility("one&two")))
            .as("'and' test")
            .isTrue();

        // test for or
        assertThat(ve.evaluate(new ElementVisibility("foor|four")))
            .as("'or' test")
            .isTrue();

        // test for and and or
        assertThat(ve.evaluate(new ElementVisibility("(one&two)|(foo&bar)")))
            .as("'and' and 'or' test")
            .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"one", "one|five", "five|one", "(one)", "(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar",
            "(one|foo)|bar", "((one|foo)|bar)&two"})
    void testFalseNegatives(String marking) throws VisibilityParseException {
        assertThat(ve.evaluate(new ElementVisibility(marking)))
            .as(marking)
            .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"five", "one&five", "five&one", "((one|foo)|bar)&goober"})
    void testFalsePositives(String marking) throws VisibilityParseException {
        assertThat(ve.evaluate(new ElementVisibility(marking)))
            .as(marking)
            .isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"one(five)", "(five)one", "(one)(two)", "a|(b(c))"})
    void testMissingSeparatorsShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @ParameterizedTest
    @ValueSource(strings = {"&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)"})
    void testUnexpectedSeparatorShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @ParameterizedTest
    @ValueSource(strings = {"(", ")", "(a&b", "b|a)"})
    void testMismatchedParenthesisShouldThrowPSX(String marking) {
        assertThatExceptionOfType(PatternSyntaxException.class).isThrownBy(() -> new ElementVisibility(marking));
    }

    @Test
    void testQuotedExpressions() throws VisibilityParseException {
        final Authorisations auths = new Authorisations("A#C", "A\"C", "A\\C", "AC");
        final VisibilityEvaluator visEv = new VisibilityEvaluator(auths);

        assertThat(visEv.evaluate(new ElementVisibility(quote("A#C") + "|" + quote("A?C")))).isTrue();

        assertThat(visEv.evaluate(new ElementVisibility(quote("A#C") + "&B"))).isFalse();

        assertThat(visEv.evaluate(new ElementVisibility(quote("A#C")))).isTrue();
        assertThat(visEv.evaluate(new ElementVisibility("(" + quote("A#C") + ")"))).isTrue();
    }

    @Test
    void testQuote() {
        assertThat(quote("A#C")).isEqualTo("\"A#C\"");
        assertThat(quote("A\"C")).isEqualTo("\"A\\\"C\"");
        assertThat(quote("A\"\\C")).isEqualTo("\"A\\\"\\\\C\"");
        assertThat(quote("ACS")).isEqualTo("ACS");
        assertThat(quote("九")).isEqualTo("\"九\"");
        assertThat(quote("五十")).isEqualTo("\"五十\"");
    }

    @Test
    void testNonAscii() throws VisibilityParseException {
        final VisibilityEvaluator visEv = new VisibilityEvaluator(new Authorisations("五", "六", "八", "九", "五十"));

        assertThat(visEv.evaluate(new ElementVisibility(quote("五") + "|" + quote("四")))).isTrue();
        assertThat(visEv.evaluate(new ElementVisibility(quote("五") + "&" + quote("四")))).isFalse();
        assertThat(visEv.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("九") + ")"))).isTrue();
        assertThat(visEv.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"五十\")"))).isTrue();
        assertThat(visEv.evaluate(new ElementVisibility(quote("五") + "&(" + quote("四") + "|" + quote("三") + ")"))).isFalse();
        assertThat(visEv.evaluate(new ElementVisibility("\"五\"&(\"四\"|\"三\")"))).isFalse();
    }
}
