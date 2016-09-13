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
package gaffer.example.function.filter;

import gaffer.function.simple.filter.IsMoreThan;

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
        runExample(new IsMoreThan(5),
                "new IsMoreThan(5)",
                1, 5, 10);
    }

    public void isMoreThanOrEqualTo5() {
        runExample(new IsMoreThan(5, true),
                "new IsMoreThan(5, true)",
                1, 5, 10);
    }

    public void isMoreThanALong5() {
        runExample(new IsMoreThan(5L),
                "new IsMoreThan(5L)",
                1, 1L, 5, 5L, 10, 10L, "abc");
    }

    public void isMoreThanAString() {
        runExample(new IsMoreThan("B"),
                "new IsMoreThan(B)",
                1, "A", "B", "C");
    }
}
