/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.util;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.function.Predicate;

/**
 * A {@code Conditional} is a simple POJO for wrapping an {@link Operation} and a {@link Predicate},
 * to allow for pre-predicate transformations, whilst preserving the input data.
 */
public class Conditional {

    private Operation transform;
    private Predicate predicate;

    public Operation getTransform() {
        return transform;
    }

    public void setTransform(final Operation transform) {
        this.transform = transform;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(final Predicate predicate) {
        this.predicate = predicate;
    }

}
