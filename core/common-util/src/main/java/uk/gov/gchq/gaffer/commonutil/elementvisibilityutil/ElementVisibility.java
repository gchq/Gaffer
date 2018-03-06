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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class is coped from org.apache.accumulo.core.security.ColumnVisibility.
 */
public class ElementVisibility {
    ElementVisibility.Node node;
    private byte[] expression;
    private static final ElementVisibility.Node EMPTY_NODE;

    static {
        EMPTY_NODE = new ElementVisibility.Node(ElementVisibility.NodeType.EMPTY, 0);
    }

    public ElementVisibility(final String expression) {
        this(expression.getBytes(StandardCharsets.UTF_8));
    }

    public ElementVisibility(final byte[] expression) {
        this.node = null;
        this.validate(expression);
    }

    public byte[] getExpression() {
        return this.expression;
    }

    public static String quote(final String term) {
        return new String(quote(term.getBytes(UTF_8)), UTF_8);
    }

    public static byte[] quote(final byte[] term) {
        boolean needsQuote = false;

        for (int i = 0; i < term.length; i++) {
            if (!Authorisations.isValidAuthChar(term[i])) {
                needsQuote = true;
                break;
            }
        }

        if (!needsQuote) {
            return term;
        }

        return VisibilityEvaluator.escape(term, true);
    }

    public String toString() {
        return "[" + new String(this.expression, StandardCharsets.UTF_8) + "]";
    }

    public boolean equals(final Object obj) {
        return obj instanceof ElementVisibility ? this.equals((ElementVisibility) obj) : false;
    }

    public boolean equals(final ElementVisibility otherLe) {
        return Arrays.equals(this.expression, otherLe.expression);
    }

    public int hashCode() {
        return Arrays.hashCode(this.expression);
    }

    public ElementVisibility.Node getParseTree() {
        return this.node;
    }

    private void validate(final byte[] expression) {
        if (expression != null && expression.length > 0) {
            ElementVisibility.ColumnVisibilityParser p = new ElementVisibility.ColumnVisibilityParser();
            this.node = p.parse(expression);
        } else {
            this.node = EMPTY_NODE;
        }

        this.expression = expression;
    }

    public static class Node {
        public static final List<Node> EMPTY = Collections.emptyList();
        ElementVisibility.NodeType type;
        int start;
        int end;
        List<ElementVisibility.Node> children;

        public Node(final ElementVisibility.NodeType type, final int start) {
            this.children = EMPTY;
            this.type = type;
            this.start = start;
            this.end = start + 1;
        }

        public Node(final int start, final int end) {
            this.children = EMPTY;
            this.type = ElementVisibility.NodeType.TERM;
            this.start = start;
            this.end = end;
        }

        public void add(final ElementVisibility.Node child) {
            if (this.children == EMPTY) {
                this.children = new ArrayList();
            }

            this.children.add(child);
        }

        public ElementVisibility.NodeType getType() {
            return this.type;
        }

        public List<ElementVisibility.Node> getChildren() {
            return this.children;
        }

        public int getTermStart() {
            return this.start;
        }

        public int getTermEnd() {
            return this.end;
        }

        public ArrayByteSequence getTerm(final byte[] expression) {
            if (this.type != ElementVisibility.NodeType.TERM) {
                throw new RuntimeException();
            } else if (expression[this.start] == 34) {
                int qStart = this.start + 1;
                int qEnd = this.end - 1;
                return new ArrayByteSequence(expression, qStart, qEnd - qStart);
            } else {
                return new ArrayByteSequence(expression, this.start, this.end - this.start);
            }
        }
    }

    public enum NodeType {
        EMPTY,
        TERM,
        OR,
        AND;

        NodeType() {
        }
    }

    private static class ColumnVisibilityParser {
        private int index = 0;
        private int parens = 0;

        ColumnVisibilityParser() {
        }

        ElementVisibility.Node parse(final byte[] expression) {
            if (expression.length > 0) {
                ElementVisibility.Node node = this.parse_(expression);
                if (node == null) {
                    throw new PatternSyntaxException("operator or missing parens", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                } else if (this.parens != 0) {
                    throw new PatternSyntaxException("parenthesis mis-match", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                } else {
                    return node;
                }
            } else {
                return null;
            }
        }

        ElementVisibility.Node processTerm(final int start, final int end, final ElementVisibility.Node expr, final byte[] expression) {
            if (start != end) {
                if (expr != null) {
                    throw new PatternSyntaxException("expression needs | or &", new String(expression, StandardCharsets.UTF_8), start);
                } else {
                    return new ElementVisibility.Node(start, end);
                }
            } else if (expr == null) {
                throw new PatternSyntaxException("empty term", new String(expression, StandardCharsets.UTF_8), start);
            } else {
                return expr;
            }
        }

        ElementVisibility.Node parse_(final byte[] expression) {
            ElementVisibility.Node result = null;
            ElementVisibility.Node expr = null;
            int wholeTermStart = this.index;
            int subtermStart = this.index;
            boolean subtermComplete = false;

            ElementVisibility.Node child;
            while (this.index < expression.length) {
                switch (expression[this.index++]) {
                    case 34:
                        if (subtermStart != this.index - 1) {
                            throw new PatternSyntaxException("expression needs & or |", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                        }

                        for (; this.index < expression.length && expression[this.index] != 34; ++this.index) {
                            if (expression[this.index] == 92) {
                                ++this.index;
                                if (expression[this.index] != 92 && expression[this.index] != 34) {
                                    throw new PatternSyntaxException("invalid escaping within quotes", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                                }
                            }
                        }

                        if (this.index == expression.length) {
                            throw new PatternSyntaxException("unclosed quote", new String(expression, StandardCharsets.UTF_8), subtermStart);
                        }

                        if (subtermStart + 1 == this.index) {
                            throw new PatternSyntaxException("empty term", new String(expression, StandardCharsets.UTF_8), subtermStart);
                        }

                        ++this.index;
                        subtermComplete = true;
                        break;
                    case 38:
                        expr = this.processTerm(subtermStart, this.index - 1, expr, expression);
                        if (result != null) {
                            if (!result.type.equals(ElementVisibility.NodeType.AND)) {
                                throw new PatternSyntaxException("cannot mix & and |", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                            }
                        } else {
                            result = new ElementVisibility.Node(ElementVisibility.NodeType.AND, wholeTermStart);
                        }

                        result.add(expr);
                        expr = null;
                        subtermStart = this.index;
                        subtermComplete = false;
                        break;
                    case 40:
                        ++this.parens;
                        if (subtermStart == this.index - 1 && expr == null) {
                            expr = this.parse_(expression);
                            subtermStart = this.index;
                            subtermComplete = false;
                            break;
                        }

                        throw new PatternSyntaxException("expression needs & or |", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                    case 41:
                        --this.parens;
                        child = this.processTerm(subtermStart, this.index - 1, expr, expression);
                        if (child == null && result == null) {
                            throw new PatternSyntaxException("empty expression not allowed", new String(expression, StandardCharsets.UTF_8), this.index);
                        }

                        if (result == null) {
                            return child;
                        }

                        if (result.type == child.type) {
                            Iterator var8 = child.children.iterator();

                            while (var8.hasNext()) {
                                ElementVisibility.Node c = (ElementVisibility.Node) var8.next();
                                result.add(c);
                            }
                        } else {
                            result.add(child);
                        }

                        result.end = this.index - 1;
                        return result;
                    case 124:
                        expr = this.processTerm(subtermStart, this.index - 1, expr, expression);
                        if (result != null) {
                            if (!result.type.equals(ElementVisibility.NodeType.OR)) {
                                throw new PatternSyntaxException("cannot mix | and &", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                            }
                        } else {
                            result = new ElementVisibility.Node(ElementVisibility.NodeType.OR, wholeTermStart);
                        }

                        result.add(expr);
                        expr = null;
                        subtermStart = this.index;
                        subtermComplete = false;
                        break;
                    default:
                        if (subtermComplete) {
                            throw new PatternSyntaxException("expression needs & or |", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                        }

                        byte var10 = expression[this.index - 1];
                        if (!Authorisations.isValidAuthChar(var10)) {
                            throw new PatternSyntaxException("bad character (" + var10 + ")", new String(expression, StandardCharsets.UTF_8), this.index - 1);
                        }
                }
            }

            child = this.processTerm(subtermStart, this.index, expr, expression);
            if (result != null) {
                result.add(child);
                result.end = this.index;
            } else {
                result = child;
            }

            if (result.type != ElementVisibility.NodeType.TERM && result.children.size() < 2) {
                throw new PatternSyntaxException("missing term", new String(expression, StandardCharsets.UTF_8), this.index);
            } else {
                return result;
            }
        }
    }
}
