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


import uk.gov.gchq.gaffer.function.filter.And;
import uk.gov.gchq.gaffer.function.filter.IsLessThan;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.koryphe.tuple.n.Tuple1;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import java.util.Arrays;

public class AndExample extends PredicateExample {
    public static void main(final String[] args) {
        new AndExample().run();
    }

    public AndExample() {
        super(And.class);
    }

    public void runExamples() {
        isLessThan3AndIsMoreThan0();
        property1IsLessThan2AndProperty2IsMoreThan2();
    }

    public void isLessThan3AndIsMoreThan0() {
        // ---------------------------------------------------------
        final And function = new And<>(Arrays.asList(
                new IsLessThan(3),
                new IsMoreThan(0)
        ));
        // ---------------------------------------------------------

        runExample(function, 0, 1, 2, 3, 1L, 2L);
    }

    public void property1IsLessThan2AndProperty2IsMoreThan2() {
        // ---------------------------------------------------------
        final And function = new And<>(Arrays.asList(
                new TupleAdaptedPredicate<>(new IsLessThan(2), 0),
                new TupleAdaptedPredicate<>(new IsMoreThan(2), 0)
        ));
        // ---------------------------------------------------------

        runExample(function,
                new Tuple2<>(1, 3),
                new Tuple2<>(1, 1),
                new Tuple2<>(3, 3),
                new Tuple2<>(3, 1),
                new Tuple2<>(1L, 3L),
                new Tuple1<>(1)
        );
    }
}
