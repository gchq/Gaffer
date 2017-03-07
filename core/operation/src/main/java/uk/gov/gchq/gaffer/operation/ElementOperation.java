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

package uk.gov.gchq.gaffer.operation;

/**
 * A <code>ElementOperation</code> defines an operation that processes elements.
 * <p>
 * ElementOperation operations have several flags to determine how to filter {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <INPUT>  the input of the operation. This must be JSON serialisable.
 * @param <OUTPUT> the result type of the operation. This must be JSON serialisable.
 */
public interface ElementOperation<INPUT, OUTPUT> extends Operation<INPUT, OUTPUT> {
    /**
     * @return directedType a {@link IncludeIncomingOutgoingType}
     * that controls the direction of {@link uk.gov.gchq.gaffer.data.element.Edge}s that are
     * filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    DirectedType getDirectedType();

    /**
     * @param directedType a {@link IncludeIncomingOutgoingType}
     *                     that controls the direction of {@link uk.gov.gchq.gaffer.data.element.Edge}s that are
     *                     filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    void setDirectedType(final DirectedType directedType);

    /**
     * @return includeIncomingOutGoing a {@link IncludeIncomingOutgoingType}
     * that controls the incoming/outgoing direction of {@link uk.gov.gchq.gaffer.data.element.Edge}s that are
     * filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    IncludeIncomingOutgoingType getIncludeIncomingOutGoing();

    /**
     * @param includeIncomingOutGoing a {@link DirectedType}
     *                                that controls the incoming/outgoing direction of {@link uk.gov.gchq.gaffer.data.element.Edge}s that are
     *                                filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing);

    /**
     * A <code>IncludeIncomingOutgoingType</code> defines the incoming/outgoing
     * direction of the {@link uk.gov.gchq.gaffer.data.element.Edge}s during
     * the operation.
     */
    enum IncludeIncomingOutgoingType {
        BOTH, INCOMING, OUTGOING
    }

    /**
     * A <code>DirectedType</code> defines whether
     * {@link uk.gov.gchq.gaffer.data.element.Edge}s used in the operation should
     * be directed.
     */
    enum DirectedType {
        BOTH, DIRECTED, UNDIRECTED
    }
}
