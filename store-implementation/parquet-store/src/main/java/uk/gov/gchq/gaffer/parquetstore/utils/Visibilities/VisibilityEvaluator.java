/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils.visibilities;

import uk.gov.gchq.gaffer.parquetstore.utils.visibilities.exception.VisibilityParseException;

public class VisibilityEvaluator {
    private Authorisations auths;

    public VisibilityEvaluator(final Authorisations auths) {
        this.auths = auths;
    }

    public boolean evaluate(final ColumnVisibility visibility) throws VisibilityParseException {
        return this.evaluate(visibility.getExpression(), visibility.getParseTree());
    }

    private boolean evaluate(final byte[] expression, final ColumnVisibility.Node root) throws VisibilityParseException {
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
                for (final ColumnVisibility.Node child : root.children) {
                    if (!evaluate(expression, child)) {
                        return false;
                    }
                }
                return true;
            case OR:
                if (root.children == null || root.children.size() < 2) {
                    throw new VisibilityParseException("OR has less than 2 children", expression, root.start);
                }
                for (final ColumnVisibility.Node child : root.children) {
                    if (evaluate(expression, child)) {
                        return true;
                    }
                }
                return false;
            default:
                throw new VisibilityParseException("No such node type", expression, root.start);
        }
    }
}
