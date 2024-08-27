package uk.gov.gchq.gaffer.federated.simple.merge;

import java.util.Collection;
import java.util.function.BinaryOperator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federated.simple.merge.operator.ElementAggregateOperator;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

public abstract class FederatedResultAccumulator<T> implements BinaryOperator<T> {
    // Default merge operators for different data types
    protected BinaryOperator<Number> numberMergeOperator = new Sum();
    protected BinaryOperator<String> stringMergeOperator = new StringConcat();
    protected BinaryOperator<Collection<Object>> collectionMergeOperator = new CollectionConcat<>();
    protected BinaryOperator<Iterable<Element>> elementMergeOperator = new ElementAggregateOperator();

    // Should the element merge operator be used
    protected boolean mergeElements = true;

    /**
     * Set whether the element merge operator should be used.
     *
     * @param mergeElements should merge.
     */
    public void setMergeElements(boolean mergeElements) {
        this.mergeElements = mergeElements;
    }
}
