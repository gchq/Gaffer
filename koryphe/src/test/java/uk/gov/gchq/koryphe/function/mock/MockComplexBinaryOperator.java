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

package uk.gov.gchq.koryphe.function.mock;

import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;
import java.util.function.BinaryOperator;

public class MockComplexBinaryOperator implements BinaryOperator<Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>> {
    @Override
    public Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> apply(
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> input,
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> state) {
        return input;
    }
}
