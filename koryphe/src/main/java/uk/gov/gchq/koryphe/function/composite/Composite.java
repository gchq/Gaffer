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

package uk.gov.gchq.koryphe.function.composite;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A <code>CompositeFunction</code> is a {@link List} of {@link Function}s that combine to make a composite
 * function.
 *
 * @param <F> The type of Function
 */
public abstract class Composite<F> extends ArrayList<F> {
    public Composite() {
        super();
    }

    public Composite(List<F> functions) {
        super(functions);
    }
}
