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
package uk.gov.gchq.gaffer.example.function.filter;

import uk.gov.gchq.gaffer.function.filter.IsLessThan;

public class IsLessThanExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new IsLessThanExample().run();
    }

    public IsLessThanExample() {
        super(IsLessThan.class);
    }

    public void runExamples() {
        isLessThan5();
        isLessThanOrEqualTo5();
        isLessThanALong5();
        isLessThanAString();
    }

    public void isLessThan5() {
        // ---------------------------------------------------------
        final IsLessThan function = new IsLessThan(5);
        // ---------------------------------------------------------

        runExample(function, 1, 1L, 5, 5L, 10, 10L, "1");
    }

    public void isLessThanOrEqualTo5() {
        // ---------------------------------------------------------
        final IsLessThan function = new IsLessThan(5, true);
        // ---------------------------------------------------------

        runExample(function, 1, 1L, 5, 5L, 10, 10L, "1");
    }

    public void isLessThanALong5() {
        // ---------------------------------------------------------
        final IsLessThan function = new IsLessThan(5L);
        // ---------------------------------------------------------

        runExample(function, 1, 1L, 5, 5L, 10, 10L, "1");
    }

    public void isLessThanAString() {
        // ---------------------------------------------------------
        final IsLessThan function = new IsLessThan("B");
        // ---------------------------------------------------------

        runExample(function, 1, "A", "B", "C");
    }
}
