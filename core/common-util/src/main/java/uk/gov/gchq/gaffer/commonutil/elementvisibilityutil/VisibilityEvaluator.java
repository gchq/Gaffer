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

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;

/**
 * This class is copied from org.apache.accumulo.core.security.VisibilityEvaluator.
 */
public class VisibilityEvaluator {
    private Authorisations auths;

    public VisibilityEvaluator(final Authorisations auths) {
        this.auths = auths;
    }

    public boolean evaluate(final ElementVisibility visibility) throws VisibilityParseException {
        return this.evaluate(visibility.getExpression(), visibility.getParseTree());
    }

    private boolean evaluate(final byte[] expression, final ElementVisibility.Node root) throws VisibilityParseException {
        if (expression.length == 0) {
            return true;
        }
        switch (root.type) {
            case TERM:
                return auths.contains(root.getTerm(expression));
            case AND:
                if (root.children == null || root.children.size() < 2) {
                    throw new VisibilityParseException("AND has less than 2 children", expression, root.start);
                }
                for (final ElementVisibility.Node child : root.children) {
                    if (!evaluate(expression, child)) {
                        return false;
                    }
                }
                return true;
            case OR:
                if (root.children == null || root.children.size() < 2) {
                    throw new VisibilityParseException("OR has less than 2 children", expression, root.start);
                }
                for (final ElementVisibility.Node child : root.children) {
                    if (evaluate(expression, child)) {
                        return true;
                    }
                }
                return false;
            default:
                throw new VisibilityParseException("No such node type", expression, root.start);
        }
    }

    public static byte[] escape(final byte[] auth, final boolean quote) {
        int escapeCount = 0;
        byte[] newAuth = auth;

        for (int i = 0; i < auth.length; i++) {
            if (auth[i] == '"' || auth[i] == '\\') {
                escapeCount++;
            }
        }

        if (escapeCount > 0 || quote) {
            byte[] escapedAuth = new byte[auth.length + escapeCount + (quote ? 2 : 0)];
            int index = quote ? 1 : 0;
            for (int i = 0; i < auth.length; i++) {
                if (auth[i] == '"' || auth[i] == '\\') {
                    escapedAuth[index++] = '\\';
                }
                escapedAuth[index++] = auth[i];
            }

            if (quote) {
                escapedAuth[0] = '"';
                escapedAuth[escapedAuth.length - 1] = '"';
            }

            newAuth = escapedAuth;
        }
        return newAuth;
    }
}
