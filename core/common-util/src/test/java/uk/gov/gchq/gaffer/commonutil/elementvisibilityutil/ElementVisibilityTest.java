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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility.quote;

/**
 * This test class is copied from org.apache.accumulo.core.security.ColumnVisibilityTest.
 */

public class ElementVisibilityTest {

    @Test
    public void testEmptyStringIsValid() {
        final ElementVisibility a = new ElementVisibility(new byte[0]);
        final ElementVisibility b = new ElementVisibility("");

        assertEquals(a, b);
    }

    @Test
    public void testCharactersOnly() {
        getBytesShouldNotThrowIAX("test", "words");
    }

    @Test
    public void testCompound() {
        getBytesShouldNotThrowIAX("a|b", "a&b", "ab&bc", "_&-&:", "A&B&C&D&E", "A|B|C|D|E", "(A|B|C)", "(A)|B|(C)", "A&(B)&(C)", "A&B&(L)");
    }

    @Test
    public void testBadCharacters() {
        getBytesShouldThrowIAX("=", "*", "^", "%", "@", "a*b");
    }

    @Test
    public void testComplexCompound() {
        getBytesShouldNotThrowIAX("(a|b)&(x|y)", "a&(x|y)", "(a|b)&(x|y)", "A&(L|M)", "B&(L|M)", "A&B&(L|M)");
        getBytesShouldNotThrowIAX("A&FOO&(L|M)", "(A|B)&FOO&(L|M)", "A&B&(L|M|FOO)", "((A|B|C)|foo)&bar");
        getBytesShouldNotThrowIAX("(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar", "((one|foo)|bar)&two");
    }

    @Test
    public void testDanglingOperators() {
        getBytesShouldThrowIAX("a|b&", "(|a)", "|", "a|", "|a", "|", "&");
        getBytesShouldThrowIAX("&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)");
    }

    @Test
    public void testMissingSeparators() {
        getBytesShouldThrowIAX("one(five)", "(five)one", "(one)(two)", "a|(b(c))");
    }

    @Test
    public void testMismatchedParentheses() {
        getBytesShouldThrowIAX("(", ")", "(a&b", "b|a)", "A|B)");
    }

    @Test
    public void testMixedOperators() {
        getBytesShouldThrowIAX("(A&B)|(C&D)&(E)", "a|b&c", "A&B&C|D", "(A&B)|(C&D)&(E)");
    }

    @Test
    public void testQuotes() {
        getBytesShouldThrowIAX("\"\"", "\"A\"A", "\"A\"\"B\"", "(A)\"B\"", "\"A\"(B)");
        getBytesShouldThrowIAX("\"A", "\"", "\"B", "A&\"B", "A&\"B\\'");

        getBytesShouldNotThrowIAX("\"A\"", "(\"A\")", "A&\"B.D\"", "A&\"B\\\\D\"", "A&\"B\\\"D\"");
    }

    @Test
    public void testToStringSimpleCharacter() {
        final ElementVisibility cv = new ElementVisibility(quote("a"));

        assertEquals("[a]", cv.toString());
    }

    @Test
    public void testToStringMultiByte() {
        final ElementVisibility cv = new ElementVisibility(quote("五"));

        assertEquals("[\"五\"]", cv.toString());
    }

    @Test
    public void testParseTree() {
        final ElementVisibility.Node node = parse("(W)|(U&V)");

        assertNode(node, ElementVisibility.NodeType.OR, 0, 9);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 2);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 8);
    }

    @Test
    public void testParseTreeWithNoChildren() {
        final ElementVisibility.Node node = parse("ABC");

        assertNode(node, ElementVisibility.NodeType.TERM, 0, 3);
    }

    @Test
    public void testParseTreeWithTwoChildren() {
        final ElementVisibility.Node node = parse("ABC|DEF");

        assertNode(node, ElementVisibility.NodeType.OR, 0, 7);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 0, 3);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.TERM, 4, 7);
    }

    @Test
    public void testParseTreeWithParenthesesAndTwoChildren() {
        final ElementVisibility.Node node = parse("(ABC|DEF)");

        assertNode(node, ElementVisibility.NodeType.OR, 1, 8);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 4);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.TERM, 5, 8);
    }

    @Test
    public void testParseTreeWithParenthesizedChildren() {
        final ElementVisibility.Node node = parse("ABC|(DEF&GHI)");

        assertNode(node, ElementVisibility.NodeType.OR, 0, 13);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 0, 3);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 12);
        assertNode(node.getChildren().get(1).children.get(0), ElementVisibility.NodeType.TERM, 5, 8);
        assertNode(node.getChildren().get(1).children.get(1), ElementVisibility.NodeType.TERM, 9, 12);
    }

    @Test
    public void testParseTreeWithMoreParentheses() {
        final ElementVisibility.Node node = parse("(W)|(U&V)");

        assertNode(node, ElementVisibility.NodeType.OR, 0, 9);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 2);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 8);
        assertNode(node.getChildren().get(1).children.get(0), ElementVisibility.NodeType.TERM, 5, 6);
        assertNode(node.getChildren().get(1).children.get(1), ElementVisibility.NodeType.TERM, 7, 8);
    }

    private void getBytesShouldThrowIAX(final String... strings) {
        Arrays.stream(strings)
                .map(String::getBytes)
                .forEach(bytes ->
                    assertThatIllegalArgumentException()
                            .isThrownBy(() -> new ElementVisibility(bytes))
                );
    }

    private void getBytesShouldNotThrowIAX(final String... strings) {
        for (String s : strings) {
            assertThatNoException().isThrownBy(() -> new ElementVisibility(s.getBytes()));
        }
    }

    private ElementVisibility.Node parse(final String s) {
        final ElementVisibility v = new ElementVisibility(s);
        return v.getParseTree();
    }

    private void assertNode(final ElementVisibility.Node node, final ElementVisibility.NodeType nodeType, final int start, final int end) {
        assertThat(node).satisfies(n -> {
                    assertThat(n.type).isEqualTo(nodeType);
                    assertThat(n.start).isEqualTo(start);
                    assertThat(n.end).isEqualTo(end);
                }
        );
    }
}
