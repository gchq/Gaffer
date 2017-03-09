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

package koryphe.tuple.function;

import koryphe.function.Function;
import koryphe.tuple.mask.TupleMask;

public abstract class TupleInputOutputFunction<R, I, O, F extends Function<I, O>> extends TupleInputFunction<R, I, O, F> {
    protected TupleMask<R, O> projection;

    public TupleInputOutputFunction() {}

    public TupleInputOutputFunction(TupleMask<R, I> inputMask, F function, TupleMask<R, O> projection) {
        super(inputMask, function);
        setProjection(projection);
    }

    public TupleMask<R, O> getProjection() {
        return projection;
    }

    public void setProjection(TupleMask<R, O> projection) {
        this.projection = projection;
    }
}
