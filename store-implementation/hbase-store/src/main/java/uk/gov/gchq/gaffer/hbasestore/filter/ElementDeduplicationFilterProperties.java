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

package uk.gov.gchq.gaffer.hbasestore.filter;

import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class ElementDeduplicationFilterProperties {
    private boolean edges = false;
    private boolean entities = false;
    private boolean unDirectedEdges = false;
    private boolean directedEdges = false;
    private boolean incomingEdges = false;
    private boolean outgoingEdges = false;
    private boolean deduplicateUndirectedEdges = false;
    private boolean skipFilter = false;

    public ElementDeduplicationFilterProperties() {
    }

    public ElementDeduplicationFilterProperties(final GetElementsOperation<?, ?> operation) {
        entities = operation.isIncludeEntities();
        edges = GetOperation.IncludeEdgeType.NONE != operation.getIncludeEdges();
        incomingEdges = GetOperation.IncludeIncomingOutgoingType.INCOMING == operation.getIncludeIncomingOutGoing();
        outgoingEdges = GetOperation.IncludeIncomingOutgoingType.OUTGOING == operation.getIncludeIncomingOutGoing();
        deduplicateUndirectedEdges = operation instanceof GetAllElements;
        directedEdges = GetOperation.IncludeEdgeType.DIRECTED == operation.getIncludeEdges();
        unDirectedEdges = GetOperation.IncludeEdgeType.UNDIRECTED == operation.getIncludeEdges();
        skipFilter = operation.getIncludeEdges() == GetOperation.IncludeEdgeType.ALL
                && operation.getIncludeIncomingOutGoing() == GetOperation.IncludeIncomingOutgoingType.BOTH
                && entities
                && !deduplicateUndirectedEdges;
    }

    public boolean isEdges() {
        return edges;
    }

    public void setEdges(final boolean edges) {
        this.edges = edges;
    }

    public boolean isEntities() {
        return entities;
    }

    public void setEntities(final boolean entities) {
        this.entities = entities;
    }

    public boolean isUnDirectedEdges() {
        return unDirectedEdges;
    }

    public void setUnDirectedEdges(final boolean unDirectedEdges) {
        this.unDirectedEdges = unDirectedEdges;
    }

    public boolean isDirectedEdges() {
        return directedEdges;
    }

    public void setDirectedEdges(final boolean directedEdges) {
        this.directedEdges = directedEdges;
    }

    public boolean isIncomingEdges() {
        return incomingEdges;
    }

    public void setIncomingEdges(final boolean incomingEdges) {
        this.incomingEdges = incomingEdges;
    }

    public boolean isOutgoingEdges() {
        return outgoingEdges;
    }

    public void setOutgoingEdges(final boolean outgoingEdges) {
        this.outgoingEdges = outgoingEdges;
    }

    public boolean isDeduplicateUndirectedEdges() {
        return deduplicateUndirectedEdges;
    }

    public void setDeduplicateUndirectedEdges(final boolean deduplicateUndirectedEdges) {
        this.deduplicateUndirectedEdges = deduplicateUndirectedEdges;
    }

    public boolean isSkipFilter() {
        return skipFilter;
    }

    public void setSkipFilter(final boolean skipFilter) {
        this.skipFilter = skipFilter;
    }
}
