/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.doc.dev.aggregator;

import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

public class VisibilityAggregator extends KorypheBinaryOperator<String> {

    @Override
    public String _apply(final String input1, final String input2) {
        validateInput(input1);
        validateInput(input2);

        if ("private".equals(input1) || "public".equals(input2)) {
            return "private";
        }

        return "public";
    }

    private void validateInput(final String input) {
        if (!("public".equals(input) || "private".equals(input))) {
            throw new IllegalArgumentException("Visibility must either be 'public' or 'private'. You supplied " + input);
        }
    }
}
