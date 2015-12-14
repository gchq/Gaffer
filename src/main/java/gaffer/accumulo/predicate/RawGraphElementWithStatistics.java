/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulo.predicate;

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.util.Date;

/**
 * A {@link GraphElementWithStatistics} that is lazily deserialised from an Accumulo
 * {@link Key} and {@link Value}.
 *
 * When this is constructed, no deserialisation occurs. When a field is required, only
 * the part of the {@link Key} necessary is deserialised. This means that if only the
 * summary type is required in an iterator to decide if this row is of interest, then
 * the minimum amount of deserialisation necessary happens.
 *
 * Note that this contains methods that give direct access to the fields of the
 * {@link Entity} or {@link Edge}, e.g. in order to find out the source type of an
 * edge, it is only necessary to call {@link #getSourceType()} rather than calling
 * <code>#getGraphElement().getEdge().getSourceType()</code>.
 *
 * This also contains the methods {@link #getFirstType()}, {@link #getFirstValue()},
 * {@link #getSecondType()} and {@link #getSecondValue()}, which give context as to
 * why this was found, e.g. if this is an {@link Edge} then the first type, value will
 * have been queried for.
 */
public class RawGraphElementWithStatistics extends GraphElementWithStatistics {

    private static final long serialVersionUID = 6645503569015008110L;

    private enum ElementType {ENTITY, EDGE, NOT_KNOWN_YET}

    private Key key;
    private Value value;
    // Lazily deserialised properties
    private ElementType elementType;
    private String entityType;
    private String entityValue;
    // Source and destination of the edge (if it represents an edge)
    private String sourceType;
    private String sourceValue;
    private String destinationType;
    private String destinationValue;
    // First and second type-values in the edge (if it represents an edge)
    private String firstType;
    private String firstValue;
    private String secondType;
    private String secondValue;
    private String summaryType;
    private String summarySubType;
    private boolean directed;
    private String visibility;
    private Date startDate;
    private Date endDate;
    private SetOfStatistics setOfStatistics;

    public RawGraphElementWithStatistics(Key key, Value value) {
        this.key = key;
        this.value = value;
        this.elementType = ElementType.NOT_KNOWN_YET;
    }

    @Override
    public boolean isEntity() {
        if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
            entityOrEdge();
        }
        switch (elementType) {
            case ENTITY: return true;
            case EDGE: return false;
            default: return false; // Will never happen
        }
    }

    @Override
    public boolean isEdge() {
        return !isEntity();
    }

    @Override
    public String getEntityType() {
        getEntityTypeAndValue();
        return entityType;
    }

    @Override
    public String getEntityValue() {
        getEntityTypeAndValue();
        return entityValue;
    }

    private void getEntityTypeAndValue() {
        if (elementType.equals(ElementType.EDGE)) {
            throw new UnsupportedOperationException("Cannot access the entity type or value of a " + this.getClass().getName() + " that is an Edge");
        }
        if (entityType == null) {
            try {
                // Entity type might be null because we don't yet know whether this is an entity or an edge,
                // or because it is an edge.
                if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
                    entityOrEdge();
                }
                if (elementType.equals(ElementType.ENTITY)) {
                    Pair<String> pair = ConversionUtils.getEntityTypeAndValueFromKey(key.getRow().toString());
                    entityType = pair.getFirst();
                    entityValue = pair.getSecond();
                } else {
                    // Need to check again whether this is an edge, as may have just found this out in the
                    // above if block.
                    throw new UnsupportedOperationException("Cannot access the entity type or value of a " + this.getClass().getName() + " that is an Edge");
                }
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising entity type and value in " + this + ": " + e);
            }
        }
    }

    @Override
    public String getSourceType() {
        getEdgeTypesAndValues();
        return sourceType;
    }

    @Override
    public String getSourceValue() {
        getEdgeTypesAndValues();
        return sourceValue;
    }

    @Override
    public String getDestinationType() {
        getEdgeTypesAndValues();
        return destinationType;
    }

    @Override
    public String getDestinationValue() {
        getEdgeTypesAndValues();
        return destinationValue;
    }

    public String getFirstType() {
        getFirstAndSecondTypesAndValues();
        return firstType;
    }

    public String getFirstValue() {
        getFirstAndSecondTypesAndValues();
        return firstValue;
    }

    public String getSecondType() {
        getFirstAndSecondTypesAndValues();
        return secondType;
    }

    public String getSecondValue() {
        getFirstAndSecondTypesAndValues();
        return secondValue;
    }

    private void getFirstAndSecondTypesAndValues() {
        if (elementType.equals(ElementType.ENTITY)) {
            throw new UnsupportedOperationException("Cannot access the edge source/destination types and values of a " + this.getClass().getName() + " that is an Entity");
        }
        if (firstType == null) {
            try {
                // First type might be null because we don't yet know whether this is an entity or an edge,
                // or because it is an entity.
                if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
                    entityOrEdge();
                }
                if (elementType.equals(ElementType.EDGE)) {
                    // Get firstType, firstValue, secondType, secondValue from row key
                    String[] result = ConversionUtils.getFirstAndSecondTypeValuesFromRowKey(key.getRow().toString());
                    firstType = result[0];
                    firstValue = result[1];
                    secondType = result[2];
                    secondValue = result[3];
                } else {
                    throw new UnsupportedOperationException("Cannot access the edge source/destination types and values of a " + this.getClass().getName() + " that is an Entity");
                }
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising edge types and values in " + this + ": " + e);
            }
        }
    }

    private void getEdgeTypesAndValues() {
        if (elementType.equals(ElementType.ENTITY)) {
            throw new UnsupportedOperationException("Cannot access the edge source/destination types and values of a " + this.getClass().getName() + " that is an Entity");
        }
        if (sourceType == null) {
            try {
                // Source type might be null because we don't yet know whether this is an entity or an edge,
                // or because it is an entity.
                if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
                    entityOrEdge();
                }
                if (elementType.equals(ElementType.EDGE)) {
                    // Get sourceType, sourceValue, destinationType, destinationValue and direction from row key
                    String[] result = new String[4];
                    directed = ConversionUtils.getSourceAndDestinationFromRowKey(key.getRow().toString(), result);
                    sourceType = result[0];
                    sourceValue = result[1];
                    destinationType = result[2];
                    destinationValue = result[3];
                    // The following logic is the same as that in the reorder() method in Edge.
                    // If edge is undirected then order the two type-values
                    // - the source should be less than the destination
                    // (comparing type first then value).
                    if (!directed) {
                        if (sourceType.compareTo(destinationType) < 0) {
                            return;
                        } else if (sourceType.compareTo(destinationType) > 0) {
                            // Flip the source and destination
                            String temp = destinationType;
                            destinationType = sourceType;
                            sourceType = temp;
                            temp = destinationValue;
                            destinationValue = sourceValue;
                            sourceValue = temp;
                        } else {
                            // Types are equal, so compare the values.
                            if (sourceValue.compareTo(destinationValue) <= 0) {
                                return;
                            } else {
                                String temp = destinationValue;
                                destinationValue = sourceValue;
                                sourceValue = temp;
                            }
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("Cannot access the edge source/destination types and values of a " + this.getClass().getName() + " that is an Entity");
                }
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising edge types and values in " + this + ": " + e);
            }
        }
    }

    private void entityOrEdge() {
        try {
            if (ConversionUtils.doesKeyRepresentEntity(key.getRow().toString())) {
                elementType = ElementType.ENTITY;
            } else {
                elementType = ElementType.EDGE;
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException working out whether key is entity in " + this + ": " + e);
        }
    }

    @Override
    public String getSummaryType() {
        getSummaryTypeAndSubType();
        return summaryType;
    }

    @Override
    public String getSummarySubType() {
        getSummaryTypeAndSubType();
        return summarySubType;
    }

    private void getSummaryTypeAndSubType() {
        if (summaryType == null) {
            try {
                Pair<String> pair = ConversionUtils.getTypeAndSubtypeFromColumnFamily(key.getColumnFamily().toString());
                summaryType = pair.getFirst();
                summarySubType = pair.getSecond();
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising summary type and subtype in " + this + ": " + e);
            }
        }
    }

    @Override
    public boolean isDirected() {
        if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
            entityOrEdge();
        }
        if (elementType.equals(ElementType.ENTITY)) {
            throw new UnsupportedOperationException("Cannot ask whether a " + this.getClass().getName() + " that is an Entity is directed");
        }
        // This is an edge - ensure we have calculated whether it is directed or not
        if (sourceType == null) {
            getEdgeTypesAndValues();
        }
        return directed;
    }

    @Override
    public String getVisibility() {
        if (visibility == null) {
            visibility = key.getColumnVisibility().toString();
        }
        return visibility;
    }

    @Override
    public Date getStartDate() {
        getDates();
        return startDate;
    }

    @Override
    public Date getEndDate() {
        getDates();
        return endDate;
    }

    private void getDates() {
        if (startDate == null) {
            try {
                Pair<Date> pair = ConversionUtils.getDatesFromColumnQualifier(key.getColumnQualifier().toString());
                startDate = pair.getFirst();
                endDate = pair.getSecond();
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising dates in " + this + ": " + e);
            }
        }
    }

    @Override
    public GraphElement getGraphElement() {
        if (elementType.equals(ElementType.NOT_KNOWN_YET)) {
            entityOrEdge();
        }
        GraphElement element = null;
        if (elementType.equals(ElementType.ENTITY)) {
            element = new GraphElement(new Entity(getEntityType(), getEntityValue(), getSummaryType(), getSummarySubType(),
                    getVisibility(), getStartDate(), getEndDate()));
        } else if (elementType.equals(ElementType.EDGE)) {
            element = new GraphElement(new Edge(getSourceType(), getSourceValue(), getDestinationType(), getDestinationValue(),
                    getSummaryType(), getSummarySubType(), isDirected(), getVisibility(), getStartDate(), getEndDate()));
        }
        return element;
    }

    @Override
    public SetOfStatistics getSetOfStatistics() {
        if (setOfStatistics == null) {
            try {
                setOfStatistics = ConversionUtils.getSetOfStatisticsFromValue(value);
            } catch (IOException e) {
                throw new RuntimeException("IOException deserialising SetOfStatistics in " + this + ": " + e);
            }
        }
        return setOfStatistics;
    }

    @Override
    public String toString() {
        return "RawGraphElementWithStatistics{Key=" + key + ", Value=" + value + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RawGraphElementWithStatistics that = (RawGraphElementWithStatistics) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public GraphElementWithStatistics clone() {
        return new RawGraphElementWithStatistics(key, value);
    }
}
