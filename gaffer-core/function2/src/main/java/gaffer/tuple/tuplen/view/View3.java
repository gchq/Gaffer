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
import gaffer.tuple.tuplen.Tuple3;

public class View3<A, B, C, R> extends View2<A, B, R> implements Tuple3<A, B, C> {
    public View3(final Reference<R> first, final Reference<R> second, final Reference<R> third) {
        super(first, second, third);
    }

    protected View3(final Reference<R>... references) {
        super(references);
        if (references.length < 3) {
            throw new IllegalStateException("Invalid number of references");
        }
    }

    @Override
    public C get2() {
        return (C) get(2);
    }

    @Override
    public void put2(final C c) {
        put(2, c);
    }
}
