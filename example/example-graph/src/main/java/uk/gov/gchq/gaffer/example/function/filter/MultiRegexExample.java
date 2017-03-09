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

import uk.gov.gchq.gaffer.function.filter.MultiRegex;
import java.util.regex.Pattern;

public class MultiRegexExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new MultiRegexExample().run();
    }

    public MultiRegexExample() {
        super(MultiRegex.class);
    }

    public void runExamples() {
        multiRegexWithPattern();
    }

    public void multiRegexWithPattern() {
        // ---------------------------------------------------------
        final MultiRegex function = new MultiRegex(new Pattern[]{Pattern.compile("[a-d]"), Pattern.compile("[0-4]")});
        // ---------------------------------------------------------

        runExample(function, "a", "z", "az", 'a', "2", 2, 2L);
    }
}
