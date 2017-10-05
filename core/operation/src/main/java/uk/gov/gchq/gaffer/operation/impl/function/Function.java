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
package uk.gov.gchq.gaffer.operation.impl.function;

import java.util.Map;

/**
 * A <code>Function</code> is a Gaffer {@link uk.gov.gchq.gaffer.operation.Operation} that can be applied in an
 * {@link uk.gov.gchq.gaffer.operation.OperationChain}.
 */
public interface Function {

    /**
     * Should return a {@link Map} of {@link uk.gov.gchq.gaffer.data.element.Edge} group
     * to an {@link uk.gov.gchq.gaffer.data.element.Element} Function,
     * eg. an ElementFilter
     * This is to enable generic handling of each of the implementations of {@link Function}.
     * @return  A map of Edge group to Element Function
     */
    Map<String, ?> getEdges();

    /**
     * Should return a {@link Map} of {@link uk.gov.gchq.gaffer.data.element.Entity} group
     * to an {@link uk.gov.gchq.gaffer.data.element.Element} Function,
     * eg. an ElementFilter
     * This is to enable generic handling of each of the implementations of {@link Function}.
     * @return  A map of Entity group to Element Function
     */
    Map<String, ?> getEntities();

}
