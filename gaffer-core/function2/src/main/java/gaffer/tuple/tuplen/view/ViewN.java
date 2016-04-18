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

package gaffer.tuple.tuplen.view;

import gaffer.tuple.view.Reference;

/**
 * A {@link gaffer.tuple.view.TupleView} that refers to more than 5 values in the wrapped tuple.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class ViewN<R> extends View5<Object, Object, Object, Object, Object, R> {
    public ViewN(final Reference<R>... references) {
        super(references);
    }
}
