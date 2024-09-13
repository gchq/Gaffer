/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Iterator;

/**
 * A Lazy implementation of the FoldStep
 * Folds to a lazy iterable
 */
public class LazyFoldStep<S> extends AbstractStep<S, Iterable<S>> {

    private boolean done = false;

    public LazyFoldStep(final Traversal.Admin<S, Iterable<S>> traversal) {
        super(traversal);
    }

    @Override
    public Admin<Iterable<S>> processNextStart() {
        if (done) {
            throw FastNoSuchElementException.instance();
        }

        // Do nothing on subsequent calls
        this.done = true;

        Iterable<S> lazyIterable = () -> new Iterator<S>() {
            @Override
            public boolean hasNext() {
                return LazyFoldStep.this.starts.hasNext();
            }

            @Override
            public S next() {
                if (!hasNext()) {
                    throw FastNoSuchElementException.instance();
                }

                return LazyFoldStep.this.starts.next().get();
            }
        };

        return this.getTraversal().getTraverserGenerator().generate(lazyIterable, (Step) this, 1L);
    }
}
