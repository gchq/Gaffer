package uk.gov.gchq.gaffer.store.schema.exception;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

import java.util.function.BinaryOperator;

import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_EXCEPTION;
import static uk.gov.gchq.gaffer.store.schema.Schema.FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S;

public class AggregationFunctionSchemaException extends SchemaException {
    private static final String AGGREGATE_FUNCTION = "aggregate function";
    private static final String SCHEMAS_CONFLICT_WITH_AGGREGATE_FUNCTION = String.format(FORMAT_UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_S, AGGREGATE_FUNCTION);

    public AggregationFunctionSchemaException(final BinaryOperator aggregateFunction, final BinaryOperator typeAggregateFunction) {
        super(String.format(FORMAT_EXCEPTION, SCHEMAS_CONFLICT_WITH_AGGREGATE_FUNCTION, aggregateFunction, typeAggregateFunction));
    }
}
