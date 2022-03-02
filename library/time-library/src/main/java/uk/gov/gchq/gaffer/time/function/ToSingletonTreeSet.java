/*
 * Copyright 2019-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.time.function;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.TreeSet;

/**
 * Creates a new {@link TreeSet} and adds the given object.
 */
@Since("1.21.0")
@Summary("Creates a new TreeSet and adds the given object")
public class ToSingletonTreeSet extends KorypheFunction<Object, TreeSet<Object>> {
    @Override
    public TreeSet<Object> apply(final Object o) {
        return CollectionUtil.treeSet(o);
    }
}
