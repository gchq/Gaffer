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


import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.tuple.n.Tuple1;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

public class OrExample extends PredicateExample {
    public static void main(final String[] args) {
        new OrExample().run();
    }

    public OrExample() {
        super(Or.class);
    }

    @Override
    public void runExamples() {
        isLessThan2EqualTo5OrIsMoreThan10();
        isLessThan2EqualTo5OrIsMoreThan10();
        firstItemIsLessThan2OrSecondItemIsMoreThan10();
    }

    public void isLessThan2EqualTo5OrIsMoreThan10() {
        // ---------------------------------------------------------
        final Or function = new Or<>(
                new IsLessThan(2),
                new IsEqual(5),
                new IsMoreThan(10)
        );
        // ---------------------------------------------------------

        runExample(function, 1, 2, 3, 5, 15, 1L, 3L, 5L);
    }

    public void firstItemIsLessThan2OrSecondItemIsMoreThan10() {
        // ---------------------------------------------------------
        final Or function = new Or.Builder()
                .select(0)
                .execute(new IsLessThan(2))
                .select(1)
                .execute(new IsMoreThan(10))
                .build();
        // ---------------------------------------------------------

        runExample(function,
                new Tuple2<>(1, 15),
                new Tuple2<>(1, 1),
                new Tuple2<>(15, 15),
                new Tuple2<>(15, 1),
                new Tuple2<>(1L, 15L),
                new Tuple1<>(1));
    }
}
