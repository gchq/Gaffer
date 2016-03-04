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

package gaffer.tuple.view;

import gaffer.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class MultiReferenceHandler<R> implements TupleHandler<R> {
    private TupleView<R> view;

    public MultiReferenceHandler(final List<R> references) {
        List<TupleHandler<R>> handlers = new ArrayList<TupleHandler<R>>(references.size());
        for (R reference : references) {
            handlers.add(new SingleReferenceHandler<R>(reference));
        }
        view = new TupleView<R>(handlers);
    }

    public Tuple get(final Tuple<R> source) {
        view.setTuple(source);
        return view;
    }

    public void set(final Tuple<R> target, final Object values) {
        view.setTuple(target);
        int i = 0;
        for (Object value : (Iterable) values) {
            view.putValue(i++, value);
        }
    }
}
