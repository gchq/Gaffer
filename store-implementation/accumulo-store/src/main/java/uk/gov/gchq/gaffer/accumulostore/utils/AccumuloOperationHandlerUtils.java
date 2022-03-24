package uk.gov.gchq.gaffer.accumulostore.utils;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler.AbstractBuilder;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler.BuilderSpecificOperation;

import static uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration.INPUT;

public class AccumuloOperationHandlerUtils {

    public static final String VIEW = "view";

    public static final String DIRECTED_TYPE = "directedType";

    public static final String INCLUDE_INCOMING_OUTGOING_TYPE = "includeIncomingOutgoing";

    public static FieldDeclaration getViewDirectedTypeFieldDeclaration() {
        return new FieldDeclaration()
                .fieldRequired(VIEW, View.class)
                .fieldOptional(DIRECTED_TYPE, DirectedType.class);
    }

    public static FieldDeclaration getInputViewDirectedTypeFieldDeclaration() {
        return getViewDirectedTypeFieldDeclaration()
                .inputRequired(Iterable.class);
    }

    public static FieldDeclaration getInputInOutTypeViewDirectedTypeFieldDeclaration() {
        return getInputViewDirectedTypeFieldDeclaration()
                .fieldOptional(INCLUDE_INCOMING_OUTGOING_TYPE, IncludeIncomingOutgoingType.class);
    }

    public static abstract class BuilderViewDirectedType<B extends AbstractBuilder<B>, H extends OperationHandler<?>>
            extends BuilderSpecificOperation<B, H> {

        public B view(final View view) {
            operation.operationArg(VIEW, view);
            return getBuilder();
        }

        public B directedType(final DirectedType directedType) {
            operation.operationArg(DIRECTED_TYPE, directedType);
            return getBuilder();
        }
    }

    public static abstract class BuilderInputViewDirectedType<B extends AbstractBuilder<B>, H extends OperationHandler<?>, I>
            extends BuilderViewDirectedType<B, H> {

        public B input(final I input) {
            operationArg(INPUT, input);
            return getBuilder();
        }
    }

    public static abstract class BuilderInOutTypeViewDirectedType<B extends AbstractBuilder<B>, H extends OperationHandler<?>>
            extends BuilderViewDirectedType<B, H> {

        public B includeIncomingOutgoingType(final IncludeIncomingOutgoingType includeIncomingOutgoing) {
            operation.operationArg(INCLUDE_INCOMING_OUTGOING_TYPE, includeIncomingOutgoing);
            return getBuilder();
        }
    }

    public static abstract class BuilderInputInOutTypeViewDirectedType<B extends AbstractBuilder<B>, H extends OperationHandler<?>, I>
            extends BuilderInOutTypeViewDirectedType<B, H> {

        public B input(final I input) {
            operationArg(INPUT, input);
            return getBuilder();
        }
    }

    private AccumuloOperationHandlerUtils() {
    }
}
