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


import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.tuple.n.Tuple1;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

public class AndExample extends PredicateExample {
    public static void main(final String[] args) {
        new AndExample().run();
    }

    public AndExample() {
        super(And.class);
    }

    @Override
    public void runExamples() {
        isLessThan3AndIsMoreThan0();
        firstItemIsLessThan2AndSecondItemIsMoreThan5();
    }

    public void isLessThan3AndIsMoreThan0() {
        // ---------------------------------------------------------
        final And function = new And<>(
                new IsLessThan(3),
                new IsMoreThan(0)
        );
        // ---------------------------------------------------------

        runExample(function, 0, 1, 2, 3, 1L, 2L);
    }

    public void firstItemIsLessThan2AndSecondItemIsMoreThan5() {
        // ---------------------------------------------------------
        final And function = new And.Builder()
                .select(0)
                .execute(new IsLessThan(2))
                .select(1)
                .execute(new IsMoreThan(5))
                .build();
        // ---------------------------------------------------------

        runExample(function,
                new Tuple2<>(1, 10),
                new Tuple2<>(1, 1),
                new Tuple2<>(10, 10),
                new Tuple2<>(10, 1),
                new Tuple2<>(1L, 10L),
                new Tuple1<>(1));
    }
}
