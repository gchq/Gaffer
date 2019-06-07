/*
 * Copyright 2017-2019 Crown Copyright
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility.quote;

/**
 * This test class is copied from org.apache.accumulo.core.security.ColumnVisibilityTest.
 */

public class ElementVisibilityTest {

    private void shouldThrow(String... strings) {
        for (String s : strings) {
            try {
                new ElementVisibility(s.getBytes());
                fail("Should throw: " + s);
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

    private void shouldNotThrow(String... strings) {
        for (String s : strings) {
            new ElementVisibility(s.getBytes());
        }
    }

    @Test
    public void testEmpty() {
        // empty visibility is valid
        ElementVisibility a = new ElementVisibility(new byte[0]);
        ElementVisibility b = new ElementVisibility("");

        assertEquals(a, b);
    }

    @Test
    public void testSimple() {
        shouldNotThrow("test", "(one)");
    }

    @Test
    public void testCompound() {
        shouldNotThrow("a|b", "a&b", "ab&bc");
        shouldNotThrow("A&B&C&D&E", "A|B|C|D|E", "(A|B|C)", "(A)|B|(C)", "A&(B)&(C)", "A&B&(L)");
        shouldNotThrow("_&-&:");
    }

    @Test
    public void testBadCharacters() {
        shouldThrow("=", "*", "^", "%", "@");
        shouldThrow("a*b");
    }

    @Test
    public void testComplexCompound() {
        shouldNotThrow("(a|b)&(x|y)");
        shouldNotThrow("a&(x|y)", "(a|b)&(x|y)", "A&(L|M)", "B&(L|M)", "A&B&(L|M)");
        shouldNotThrow("A&FOO&(L|M)", "(A|B)&FOO&(L|M)", "A&B&(L|M|FOO)", "((A|B|C)|foo)&bar");
        shouldNotThrow("(one&two)|(foo&bar)", "(one|foo)&three", "one|foo|bar", "(one|foo)|bar", "((one|foo)|bar)&two");
    }

    @Test
    public void testDanglingOperators() {
        shouldThrow("a|b&");
        shouldThrow("(|a)");
        shouldThrow("|");
        shouldThrow("a|", "|a", "|", "&");
        shouldThrow("&(five)", "|(five)", "(five)&", "five|", "a|(b)&", "(&five)", "(five|)");
    }

    @Test
    public void testMissingSeparators() {
        shouldThrow("one(five)", "(five)one", "(one)(two)", "a|(b(c))");
    }

    @Test
    public void testMismatchedParentheses() {
        shouldThrow("(", ")", "(a&b", "b|a)", "A|B)");
    }

    @Test
    public void testMixedOperators() {
        shouldThrow("(A&B)|(C&D)&(E)");
        shouldThrow("a|b&c", "A&B&C|D", "(A&B)|(C&D)&(E)");
    }

    @Test
    public void testQuotes() {
        shouldThrow("\"\"");
        shouldThrow("\"A\"A");
        shouldThrow("\"A\"\"B\"");
        shouldThrow("(A)\"B\"");
        shouldThrow("\"A\"(B)");
        shouldThrow("\"A");
        shouldThrow("\"");
        shouldThrow("\"B");
        shouldThrow("A&\"B");
        shouldThrow("A&\"B\\'");

        shouldNotThrow("\"A\"");
        shouldNotThrow("(\"A\")");
        shouldNotThrow("A&\"B.D\"");
        shouldNotThrow("A&\"B\\\\D\"");
        shouldNotThrow("A&\"B\\\"D\"");
    }

    @Test
    public void testToString() {
        ElementVisibility cv = new ElementVisibility(quote("a"));
        assertEquals("[a]", cv.toString());

        // multi-byte
        cv = new ElementVisibility(quote("五"));
        assertEquals("[\"五\"]", cv.toString());
    }

    @Test
    public void testParseTree() {
        ElementVisibility.Node node = parse("(W)|(U&V)");
        assertNode(node, ElementVisibility.NodeType.OR, 0, 9);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 2);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 8);
    }

    @Test
    public void testParseTreeWithNoChildren() {
        ElementVisibility.Node node = parse("ABC");
        assertNode(node, ElementVisibility.NodeType.TERM, 0, 3);
    }

    @Test
    public void testParseTreeWithTwoChildren() {
        ElementVisibility.Node node = parse("ABC|DEF");
        assertNode(node, ElementVisibility.NodeType.OR, 0, 7);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 0, 3);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.TERM, 4, 7);
    }

    @Test
    public void testParseTreeWithParenthesesAndTwoChildren() {
        ElementVisibility.Node node = parse("(ABC|DEF)");
        assertNode(node, ElementVisibility.NodeType.OR, 1, 8);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 4);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.TERM, 5, 8);
    }

    @Test
    public void testParseTreeWithParenthesizedChildren() {
        ElementVisibility.Node node = parse("ABC|(DEF&GHI)");
        assertNode(node, ElementVisibility.NodeType.OR, 0, 13);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 0, 3);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 12);
        assertNode(node.getChildren().get(1).children.get(0), ElementVisibility.NodeType.TERM, 5, 8);
        assertNode(node.getChildren().get(1).children.get(1), ElementVisibility.NodeType.TERM, 9, 12);
    }

    @Test
    public void testParseTreeWithMoreParentheses() {
        ElementVisibility.Node node = parse("(W)|(U&V)");
        assertNode(node, ElementVisibility.NodeType.OR, 0, 9);
        assertNode(node.getChildren().get(0), ElementVisibility.NodeType.TERM, 1, 2);
        assertNode(node.getChildren().get(1), ElementVisibility.NodeType.AND, 5, 8);
        assertNode(node.getChildren().get(1).children.get(0), ElementVisibility.NodeType.TERM, 5, 6);
        assertNode(node.getChildren().get(1).children.get(1), ElementVisibility.NodeType.TERM, 7, 8);
    }


    private ElementVisibility.Node parse(String s) {
        ElementVisibility v = new ElementVisibility(s);
        return v.getParseTree();
    }

    private void assertNode(ElementVisibility.Node node, ElementVisibility.NodeType nodeType, int start, int end) {
        assertEquals(node.type, nodeType);
        assertEquals(start, node.start);
        assertEquals(end, node.end);
    }
}
