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

import uk.gov.gchq.gaffer.function.filter.IsEqual;

public class IsEqualExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new IsEqualExample().run();
    }

    public IsEqualExample() {
        super(IsEqual.class);
    }

    public void runExamples() {
        isEqualTo5();
        isEqualToString5();
        isEqualLong5();
    }

    public void isEqualTo5() {
        // ---------------------------------------------------------
        final IsEqual function = new IsEqual(5);
        // ---------------------------------------------------------

        runExample(function, 5, 5L, "5", '5');
    }

    public void isEqualToString5() {
        // ---------------------------------------------------------
        final IsEqual function = new IsEqual("5");
        // ---------------------------------------------------------

        runExample(function, 5, 5L, "5", '5');
    }

    public void isEqualLong5() {
        // ---------------------------------------------------------
        final IsEqual function = new IsEqual(5L);
        // ---------------------------------------------------------

        runExample(function, 5, 5L, "5", '5');
    }
}
