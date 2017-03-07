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

import uk.gov.gchq.gaffer.function.filter.IsMoreThan;

public class IsMoreThanExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new IsMoreThanExample().run();
    }

    public IsMoreThanExample() {
        super(IsMoreThan.class);
    }

    public void runExamples() {
        isMoreThan5();
        isMoreThanOrEqualTo5();
        isMoreThanALong5();
        isMoreThanAString();
    }

    public void isMoreThan5() {
        // ---------------------------------------------------------
        final IsMoreThan function = new IsMoreThan(5);
        // ---------------------------------------------------------

        runExample(function, 1, 5, 10);
    }

    public void isMoreThanOrEqualTo5() {
        // ---------------------------------------------------------
        final IsMoreThan function = new IsMoreThan(5, true);
        // ---------------------------------------------------------

        runExample(function, 1, 5, 10);
    }

    public void isMoreThanALong5() {
        // ---------------------------------------------------------
        final IsMoreThan function = new IsMoreThan(5L);
        // ---------------------------------------------------------

        runExample(function, 1, 1L, 5, 5L, 10, 10L, "abc");
    }

    public void isMoreThanAString() {
        // ---------------------------------------------------------
        final IsMoreThan function = new IsMoreThan("B");
        // ---------------------------------------------------------

        runExample(function, 1, "A", "B", "C");
    }
}
