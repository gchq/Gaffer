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

package uk.gov.gchq.koryphe.binaryoperator;

import uk.gov.gchq.koryphe.tuple.n.Tuple2;

public class MockTupleBinaryOperator extends KorypheBinaryOperator<Tuple2<Integer, String>> {
    @Override
    protected Tuple2<Integer, String> _apply(final Tuple2<Integer, String> a, final Tuple2<Integer, String> b) {
        return new Tuple2<>(a.get0() + b.get0(), a.get1() + "," + b.get1());
    }
}
