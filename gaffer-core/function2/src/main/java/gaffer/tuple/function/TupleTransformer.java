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

package gaffer.tuple.function;

import gaffer.tuple.Tuple;
import gaffer.function2.Transformer;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.view.TupleView;

import java.util.ArrayList;
import java.util.List;

public class TupleTransformer<R> extends Transformer<Tuple<R>, Tuple<R>> {
    private List<FunctionContext<Transformer, R>> transforms;

    public TupleTransformer() { }

    public TupleTransformer(final List<FunctionContext<Transformer, R>> transforms) {
        setTransforms(transforms);
    }

    public void setTransforms(final List<FunctionContext<Transformer, R>> transforms) {
        this.transforms = transforms;
    }

    public void addTransform(final FunctionContext<Transformer, R> transform) {
        if (transforms == null) {
            transforms = new ArrayList<FunctionContext<Transformer, R>>();
        }
        transforms.add(transform);
    }

    public void addTransform(final TupleView<R> selection, final Transformer transform, final TupleView<R> projection) {
        FunctionContext<Transformer, R> context = new FunctionContext<Transformer, R>(selection, transform, projection);
        addTransform(context);
    }

    public Tuple<R> transform(final Tuple<R> input) {
        if (transforms != null) {
            for (FunctionContext<Transformer, R> transform : transforms) {
                transform.projectInto(input, transform.getFunction().execute(transform.selectFrom(input)));
            }
        }
        return input;
    }

    public TupleTransformer<R> copy() {
        TupleTransformer<R> copy = new TupleTransformer<R>();
        for (FunctionContext<Transformer, R> transform : this.transforms) {
            copy.addTransform(transform.copy());
        }
        return copy;
    }
}
