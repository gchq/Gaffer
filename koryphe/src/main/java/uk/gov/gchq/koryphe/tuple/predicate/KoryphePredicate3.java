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
package uk.gov.gchq.koryphe.tuple.predicate;

import uk.gov.gchq.koryphe.tuple.n.Tuple3;

public abstract class KoryphePredicate3<T, U, V> extends KoryphePredicateN<Tuple3<T, U, V>> {
    public abstract boolean test(final T t, final U u, final V v);

    @Override
    protected boolean delegateTest(final Tuple3<T, U, V> tuple) {
        return test(tuple.get0(), tuple.get1(), tuple.get2());
    }
}
