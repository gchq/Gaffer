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
package uk.gov.gchq.gaffer.doc.predicate;

import uk.gov.gchq.koryphe.impl.predicate.IsXMoreThanY;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

public class IsXMoreThanYExample extends PredicateExample {
    public static void main(final String[] args) {
        new IsXMoreThanYExample().run();
    }

    public IsXMoreThanYExample() {
        super(IsXMoreThanY.class);
    }

    @Override
    public void runExamples() {
        isXMoreThanY();
    }

    public void isXMoreThanY() {
        // ---------------------------------------------------------
        final IsXMoreThanY function = new IsXMoreThanY();
        // ---------------------------------------------------------

        runExample(function,
                new Tuple2<>(1, 5),
                new Tuple2<>(5, 5),
                new Tuple2<>(10, 5),
                new Tuple2<>(10L, 5),
                new Tuple2<>(1L, 5L),
                new Tuple2<>(5L, 5L),
                new Tuple2<>(10L, 5L),
                new Tuple2<>(10, 5L),
                new Tuple2<>("bcd", "cde"),
                new Tuple2<>("bcd", "abc"),
                new Tuple2<>("10", 5)
        );
    }
}
