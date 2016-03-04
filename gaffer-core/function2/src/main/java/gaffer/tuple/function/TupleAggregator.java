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

import gaffer.function2.Buffer;
import gaffer.function2.Aggregator;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.view.TupleView;

import java.util.ArrayList;
import java.util.List;

public class TupleAggregator<R> extends Aggregator<Tuple<R>> {
    private List<FunctionContext<Buffer, R>> buffers;
    private Tuple<R> outputTuple;

    public TupleAggregator() { }

    public TupleAggregator(final List<FunctionContext<Buffer, R>> buffers) {
        setBuffers(buffers);
        outputTuple = null;
    }

    public void setBuffers(final List<FunctionContext<Buffer, R>> buffers) {
        this.buffers = buffers;
    }

    public void addBuffer(final FunctionContext<Buffer, R> buffer) {
        if (buffers == null) {
            buffers = new ArrayList<FunctionContext<Buffer, R>>();
        }
        buffers.add(buffer);
    }

    public void addBuffer(final TupleView<R> selection, final Buffer buffer, final TupleView<R> projection) {
        FunctionContext<Buffer, R> context = new FunctionContext<Buffer, R>(selection, buffer, projection);
        addBuffer(context);
    }

    public void initialise() {
        outputTuple = null;
        if (buffers != null) {
            for (FunctionContext<Buffer, R> buffer : buffers) {
                buffer.getFunction().initialise();
            }
        }
    }

    public void aggregate(final Tuple<R> input) {
        if (buffers != null) {
            if (outputTuple == null) {
                outputTuple = input;
            }
            for (FunctionContext<Buffer, R> buffer : buffers) {
                buffer.getFunction().accept(buffer.selectFrom(input));
            }
        }
    }

    public Tuple<R> aggregateGroup(final Iterable<Tuple<R>> group) {
        initialise();
        for (Tuple<R> input : group) {
            aggregate(input);
        }
        return state();
    }

    public Tuple<R> state() {
        if (outputTuple != null) {
            for (FunctionContext<Buffer, R> buffer : buffers) {
                buffer.projectInto(outputTuple, buffer.getFunction().state());
            }
        }
        return outputTuple;
    }

    public TupleAggregator<R> copy() {
        TupleAggregator<R> copy = new TupleAggregator<R>();
        for (FunctionContext<Buffer, R> buffer : this.buffers) {
            copy.addBuffer(buffer.copy());
        }
        return copy;
    }
}
