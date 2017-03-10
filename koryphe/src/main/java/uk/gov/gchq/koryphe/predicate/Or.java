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

import com.google.common.collect.Lists;
import uk.gov.gchq.koryphe.composite.Composite;
import java.util.List;
import java.util.function.Predicate;

/**
 * A composite {@link Predicate} that returns true if any of it's predicates return true, otherwise false.
 *
 * @param <I> Type of input to be validated.
 */
public final class Or<I> extends Composite<Predicate<I>> implements IKorphePredicate<I> {
    public Or() {
        super();
    }

    public Or(Predicate<I>... predicates) {
        super(Lists.newArrayList(predicates));
    }

    public Or(final List<Predicate<I>> predicates) {
        super(predicates);
    }

    @Override
    public boolean test(final I input) {
        for (Predicate<I> validator : getFunctions()) {
            if (validator.test(input)) {
                return true;
            }
        }
        return false;
    }
}
