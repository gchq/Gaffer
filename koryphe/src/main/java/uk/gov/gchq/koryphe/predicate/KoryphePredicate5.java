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
package uk.gov.gchq.koryphe.predicate;

import uk.gov.gchq.koryphe.tuple.n.Tuple5;

public abstract class KoryphePredicate5<T, U, V, W, X> extends KoryphePredicate<Tuple5<T, U, V, W, X>> {
    @Override
    public boolean test(final Tuple5<T, U, V, W, X> tuple) {
        return test(tuple.get0(), tuple.get1(), tuple.get2(), tuple.get3(), tuple.get4());
    }

    public abstract boolean test(final T t, final U u, final V v, final W w, final X x);
}
