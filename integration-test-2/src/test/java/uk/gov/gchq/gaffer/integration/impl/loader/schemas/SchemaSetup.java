package uk.gov.gchq.gaffer.integration.impl.loader.schemas;

import uk.gov.gchq.gaffer.store.schema.TestSchema;

import static uk.gov.gchq.gaffer.store.schema.TestSchema.AGGREGATION_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.BASIC_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.FULL_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.VISIBILITY_SCHEMA;

public enum SchemaSetup {

    AGGREGATION(AGGREGATION_SCHEMA, new AggregationSchemaLoader()),
    BASIC(BASIC_SCHEMA, new BasicSchemaLoader()),
    VISIBILITY(VISIBILITY_SCHEMA, new VisibilitySchemaLoader()),
    FULL(FULL_SCHEMA, new FullSchemaLoader());

    private final ISchemaLoader schemaLoader;
    private final TestSchema testSchema;

    SchemaSetup(final TestSchema testSchema, final ISchemaLoader loader) {
        this.testSchema = testSchema;
        this.schemaLoader = loader;
    }

    public TestSchema getTestSchema() {
        return testSchema;
    }

    public ISchemaLoader getLoader() {
        return schemaLoader;
    }
}
