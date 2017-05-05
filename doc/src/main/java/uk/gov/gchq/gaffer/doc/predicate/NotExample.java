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


import uk.gov.gchq.koryphe.impl.predicate.AreEqual;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

public class NotExample extends PredicateExample {
    public static void main(final String[] args) {
        new NotExample().run();
    }

    public NotExample() {
        super(Not.class);
    }

    @Override
    public void runExamples() {
        doesNotExist();
        areNotEqual();
    }

    public void doesNotExist() {
        // ---------------------------------------------------------
        final Not function = new Not<>(new Exists());
        // ---------------------------------------------------------

        runExample(function, 1, null, "", "abc");
    }

    public void areNotEqual() {
        // ---------------------------------------------------------
        final Not function = new Not<>(new AreEqual());
        // ---------------------------------------------------------

        runExample(function,
                new Tuple2<>(1, 1.0),
                new Tuple2<>(1, 2),
                new Tuple2<>(2.5, 2.5),
                new Tuple2<>("", null),
                new Tuple2<>("abc", "abc"));
    }
}
