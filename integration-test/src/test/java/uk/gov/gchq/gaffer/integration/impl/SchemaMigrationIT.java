/*
 * Copyright 2018-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Entity.Builder;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.migrate.MigrateElement;
import uk.gov.gchq.gaffer.graph.hook.migrate.SchemaMigration;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Min;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.Arrays;
import java.util.Collections;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class SchemaMigrationIT extends AbstractStoreIT {
    static final String COUNT = "count";

    public static final Entity ENTITY_OLD = new Builder()
            .group("entityOld")
            .vertex("oldVertex")
            .property(COUNT, 10)
            .build();
    public static final Entity ENTITY_OLD_MIGRATED_TO_NEW = new Builder()
            .group("entityNew")
            .vertex("oldVertex")
            .property(COUNT, 10L)
            .build();
    public static final Entity ENTITY_NEW = new Builder()
            .group("entityNew")
            .vertex("newVertex")
            .property(COUNT, 10L)
            .build();
    public static final Entity ENTITY_NEW_MIGRATED_TO_OLD = new Builder()
            .group("entityOld")
            .vertex("newVertex")
            .property(COUNT, 10)
            .build();
    public static final Edge EDGE_OLD = new Edge.Builder()
            .group("edgeOld")
            .source("oldVertex")
            .dest("oldVertex2")
            .directed(true)
            .property(COUNT, 10)
            .build();
    public static final Edge EDGE_OLD_MIGRATED_TO_NEW = new Edge.Builder()
            .group("edgeNew")
            .source("oldVertex")
            .dest("oldVertex2")
            .directed(true)
            .property(COUNT, 10L)
            .build();
    public static final Edge EDGE_NEW = new Edge.Builder()
            .group("edgeNew")
            .source("newVertex")
            .dest("newVertex2")
            .directed(true)
            .property(COUNT, 10L)
            .build();
    public static final Edge EDGE_NEW_MIGRATED_TO_OLD = new Edge.Builder()
            .group("edgeOld")
            .source("newVertex")
            .dest("newVertex2")
            .directed(true)
            .property(COUNT, 10)
            .build();
    public static final Edge EDGE_OLD_OP_CHAIN_MIGRATED_TO_NEW = new Edge.Builder()
            .group("edgeNewOpChain")
            .source("opChainVertex")
            .dest("opChainVertex2")
            .directed(true)
            .property(COUNT, 14L)
            .build();
    public static final Edge EDGE_OLD_OP_CHAIN = new Edge.Builder()
            .group("edgeOldOpChain")
            .source("opChainVertex")
            .dest("opChainVertex2")
            .directed(true)
            .property(COUNT, 14)
            .build();
    public static final Edge EDGE_OLD_AGGREGATION = new Edge.Builder()
            .group("edgeAgg")
            .source("aggVertex")
            .dest("aggVertex2")
            .directed(true)
            .property(COUNT, 10)
            .build();
    public static final Edge EDGE_OLD_MIGRATED_TO_NEW_AGGREGATION = new Edge.Builder()
            .group("edgeAggNew")
            .source("aggVertex")
            .dest("aggVertex2")
            .directed(true)
            .property(COUNT, 10L)
            .build();
    public static final Edge EDGE_OLD_AGGREGATION_ALT_COUNT = new Edge.Builder()
            .group("edgeAgg")
            .source("aggVertex")
            .dest("aggVertex2")
            .directed(true)
            .property(COUNT, 12)
            .build();

    public static final Edge EDGE_OLD_AGGREGATION_ALT_COUNT_MIGRATED_TO_NEW = new Edge.Builder()
            .group("edgeAggNew")
            .source("aggVertex")
            .dest("aggVertex2")
            .directed(true)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .property(COUNT, 12L)
            .build();

    public static final Edge EDGE_OLD_POST_OP_AGGREGATION = new Edge.Builder()
            .group("oldEdgePostOpAgg")
            .source("postOpAggVertex")
            .dest("postOpAggVertex2")
            .directed(false)
            .property(COUNT, 5)
            .build();

    public static final Edge EDGE_OLD_POST_OP_AGGREGATION_MIGRATED_TO_NEW = new Edge.Builder()
            .group("newEdgePostOpAgg")
            .source("postOpAggVertex")
            .dest("postOpAggVertex2")
            .directed(false)
            .property(COUNT, 5L)
            .build();


    public static final Edge EDGE_NEW_POST_OP_AGGREGATION = new Edge.Builder()
            .group("newEdgePostOpAgg")
            .source("postOpAggVertex")
            .dest("postOpAggVertex2")
            .directed(false)
            .property(COUNT, 3L)
            .build();

    public static final Edge EDGE_NEW_POST_OP_AGGREGATION_AGGREGATED = new Edge.Builder()
            .group("newEdgePostOpAgg")
            .source("postOpAggVertex")
            .dest("postOpAggVertex2")
            .directed(false)
            .matchedVertex(EdgeId.MatchedVertex.SOURCE)
            .property(COUNT, 8L)
            .build();

    public static final Edge EDGE_OLD_AGG_BEFORE_POST_FILTER = new Edge.Builder()
            .group("oldEdgeAggBeforePostFilter")
            .source("aggBeforePostFilterVertex")
            .dest("aggBeforePostFilterVertex2")
            .directed(false)
            .property(COUNT, 7)
            .build();

    public static final Edge EDGE_NEW_AGG_BEFORE_POST_FILTER = new Edge.Builder()
            .group("newEdgeAggBeforePostFilter")
            .source("aggBeforePostFilterVertex")
            .dest("aggBeforePostFilterVertex2")
            .directed(false)
            .property(COUNT, 8L)
            .build();

    public static final Edge EDGE_OLD_AGG_BEFORE_POST_FILTER_AGGREGATED_WITH_NEW = new Edge.Builder()
            .group("oldEdgeAggBeforePostFilter")
            .source("aggBeforePostFilterVertex")
            .dest("aggBeforePostFilterVertex2")
            .directed(false)
            .property(COUNT, 15)
            .build();

    public static final View OLD_ENTITY_VIEW = new View.Builder()
            .entity("entityOld", new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(1))
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(2))
                            .build())
                    .build())
            .build();

    public static final View OLD_EDGE_VIEW = new View.Builder()
            .edge("edgeOld", new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(1))
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(2))
                            .build())
                    .build())
            .build();

    public static final View NEW_ENTITY_VIEW = new View.Builder()
            .entity("entityNew", new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(1L))
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(2L))
                            .build())
                    .build())
            .build();

    public static final View NEW_EDGE_VIEW = new View.Builder()
            .edge("edgeNew", new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(1L))
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(2L))
                            .build())
                    .build())
            .build();

    public static final View NEW_EDGE_VIEW_OP_CHAIN = new View.Builder()
            .edge("edgeNewOpChain", new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(13L))
                            .build())
                    .build())
            .build();

    public static final View OLD_VIEW = new View.Builder()
            .merge(OLD_ENTITY_VIEW)
            .merge(OLD_EDGE_VIEW)
            .build();

    public static final View NEW_VIEW = new View.Builder()
            .merge(NEW_ENTITY_VIEW)
            .merge(NEW_EDGE_VIEW)
            .build();

    public static final View FULL_VIEW = new View.Builder()
            .merge(OLD_VIEW)
            .merge(NEW_VIEW)
            .build();

    public static final View FULL_VIEW_OP_CHAIN = new View.Builder()
            .merge(FULL_VIEW)
            .merge(NEW_EDGE_VIEW_OP_CHAIN)
            .build();

    public static final View NEW_EDGE_AGG_VIEW = new View.Builder()
            .edge("edgeAgg", new ViewElementDefinition.Builder()
                    .groupBy()
                    .aggregator(new ElementAggregator.Builder()
                            .select(COUNT)
                            .execute(new Min())
                            .build())
                    .build())
            .build();

    public static final View EDGE_POST_OP_AGG_VIEW = new View.Builder()
            .edge("newEdgePostOpAgg")
            .build();

    public static final View EDGE_POST_AGG_FILTER_VIEW = new View.Builder()
            .edge("edgeNew", new ViewElementDefinition.Builder()
                    .postAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(11L))
                            .build())
                    .build())
            .edge("edgeAggNew", new ViewElementDefinition.Builder()
                    .postAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(11L))
                            .build())
                    .build())
            .build();

    public static final View EDGE_AGG_AND_POST_FILTER_VIEW = new View.Builder()
            .edge("oldEdgeAggBeforePostFilter", new ViewElementDefinition.Builder()
                    .postAggregationFilter(new ElementFilter.Builder()
                            .select(COUNT)
                            .execute(new IsMoreThan(11))
                            .build())
                    .build())
            .build();

    private SchemaMigration migration;

    @Override
    public void _setup() throws Exception {
        addExtraElements();
    }

    //--- Output NEW ---

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateOldToNew() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(OLD_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD_MIGRATED_TO_NEW,
                        ENTITY_NEW,
                        EDGE_OLD_MIGRATED_TO_NEW,
                        EDGE_NEW
                ),
                results);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateNewToNew() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(NEW_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD_MIGRATED_TO_NEW,
                        ENTITY_NEW,
                        EDGE_OLD_MIGRATED_TO_NEW,
                        EDGE_NEW
                ),
                results);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateOldAndNewToNew() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(FULL_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD_MIGRATED_TO_NEW,
                        ENTITY_NEW,
                        EDGE_OLD_MIGRATED_TO_NEW,
                        EDGE_NEW
                ),
                results);
    }

    //--- Output OLD ---

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateOldToOld() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.OLD);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(OLD_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD,
                        ENTITY_NEW_MIGRATED_TO_OLD,
                        EDGE_OLD,
                        EDGE_NEW_MIGRATED_TO_OLD
                ),
                results);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateNewToOld() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.OLD);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(NEW_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD,
                        ENTITY_NEW_MIGRATED_TO_OLD,
                        EDGE_OLD,
                        EDGE_NEW_MIGRATED_TO_OLD
                ),
                results);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldMigrateOldAndNewToOld() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.OLD);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("oldVertex", "newVertex")
                        .view(FULL_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        ENTITY_OLD,
                        ENTITY_NEW_MIGRATED_TO_OLD,
                        EDGE_OLD,
                        EDGE_NEW_MIGRATED_TO_OLD
                ),
                results);
    }

    @TraitRequirement({StoreTrait.TRANSFORMATION, StoreTrait.QUERY_AGGREGATION})
    @Test
    public void shouldMigrateOldToNewWithAgg() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("aggVertex")
                        .view(NEW_EDGE_AGG_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        EDGE_OLD_MIGRATED_TO_NEW_AGGREGATION
                ),
                results);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldCorrectlyApplyPostAggFiltering() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> results = graph.execute(
                new GetElements.Builder()
                        .input("aggVertex")
                        .view(EDGE_POST_AGG_FILTER_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEqualsIncludingMatchedVertex(
                Arrays.asList(
                        EDGE_OLD_AGGREGATION_ALT_COUNT_MIGRATED_TO_NEW
                ),
                results
        );
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldApplyPostOpAggregation() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // When
        final Iterable<? extends Element> resultsNoPostOpAgg = graph.execute(
                new GetElements.Builder()
                        .input("postOpAggVertex")
                        .view(EDGE_POST_OP_AGG_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        EDGE_OLD_POST_OP_AGGREGATION_MIGRATED_TO_NEW,
                        EDGE_NEW_POST_OP_AGGREGATION
                ),
                resultsNoPostOpAgg);

        // When
        migration.setAggregateAfter(true);

        final Iterable<? extends Element> resultsWithPostOpAgg = graph.execute(
                new GetElements.Builder()
                        .input("postOpAggVertex")
                        .view(EDGE_POST_OP_AGG_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEqualsIncludingMatchedVertex(
                Arrays.asList(
                        EDGE_NEW_POST_OP_AGGREGATION_AGGREGATED
                ),
                resultsWithPostOpAgg);
    }

    @TraitRequirement(StoreTrait.TRANSFORMATION)
    @Test
    public void shouldAggBeforePostFilters() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.OLD);
        migration.setAggregateAfter(true);

        // When
        final Iterable<? extends Element> resultsWithPostAgg = graph.execute(
                new GetElements.Builder()
                        .input("aggBeforePostFilterVertex")
                        .view(EDGE_AGG_AND_POST_FILTER_VIEW)
                        .build(),
                new User());

        // Then
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        EDGE_OLD_AGG_BEFORE_POST_FILTER_AGGREGATED_WITH_NEW
                ),
                resultsWithPostAgg
        );
    }

    @TraitRequirement({StoreTrait.TRANSFORMATION, StoreTrait.POST_TRANSFORMATION_FILTERING})
    @Test
    public void shouldAddOperationsAfterEachGetElements() throws OperationException {
        migration.setOutputType(SchemaMigration.MigrationOutputType.NEW);

        // Given
        GetElements getElements1 = new GetElements.Builder()
                .input("newVertex", "opChainVertex")
                .view(FULL_VIEW_OP_CHAIN)
                .build();

        GetElements getElements2 = new GetElements.Builder()
                .input("opChainVertex")
                .view(NEW_EDGE_VIEW_OP_CHAIN)
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(
                new OperationChain.Builder()
                        .first(getElements1)
                        .then(getElements2)
                        .build(), new User());

        ElementUtil.assertElementEquals(Arrays.asList(
                EDGE_OLD_OP_CHAIN_MIGRATED_TO_NEW
                ),
                results);
    }

    protected Schema createSchema() {
        return new Schema.Builder()
                .entity("entityOld", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(COUNT, "int")
                        .build())
                .entity("entityNew", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(COUNT, "long")
                        .build())
                .edge("edgeOld", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "int")
                        .build())
                .edge("edgeNew", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "long")
                        .build())
                .edge("edgeAgg", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "int")
                        .groupBy(COUNT)
                        .build())
                .edge("edgeAggNew", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "long")
                        .groupBy(COUNT)
                        .build())
                .edge("edgeOldOpChain", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "int")
                        .groupBy(COUNT)
                        .build())
                .edge("edgeNewOpChain", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "long")
                        .groupBy(COUNT)
                        .build())
                .edge("oldEdgePostOpAgg", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "int")
                        .build())
                .edge("newEdgePostOpAgg", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "long")
                        .build())
                .edge("oldEdgeAggBeforePostFilter", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "int")
                        .build())
                .edge("newEdgeAggBeforePostFilter", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("either")
                        .property(COUNT, "long")
                        .build())
                .type("string", String.class)
                .type("either", Boolean.class)
                .type("int", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type("long", new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .build();
    }

    public void addExtraElements() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(ENTITY_OLD, ENTITY_NEW, EDGE_OLD, EDGE_NEW, EDGE_OLD_OP_CHAIN, EDGE_OLD_AGGREGATION, EDGE_OLD_AGGREGATION_ALT_COUNT,
                        EDGE_OLD_POST_OP_AGGREGATION, EDGE_NEW_POST_OP_AGGREGATION, EDGE_OLD_AGG_BEFORE_POST_FILTER,
                        EDGE_NEW_AGG_BEFORE_POST_FILTER)
                .build(), new User());
    }

    @Override
    protected GraphConfig createGraphConfig() {
        migration = createMigration();
        return new GraphConfig.Builder()
                .graphId("graph1")
                .addHook(migration)
                .build();
    }

    private SchemaMigration createMigration() {
        final SchemaMigration migration = new SchemaMigration();

        migration.setEntities(Collections.singletonList(
                new MigrateElement(
                        MigrateElement.ElementType.ENTITY,
                        "entityOld",
                        "entityNew",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                )
        ));
        migration.setEdges(Arrays.asList(
                new MigrateElement(
                        MigrateElement.ElementType.EDGE,
                        "edgeOld",
                        "edgeNew",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                ),
                new MigrateElement(
                        MigrateElement.ElementType.EDGE,
                        "edgeAgg",
                        "edgeAggNew",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                ),
                new MigrateElement(
                        MigrateElement.ElementType.EDGE,
                        "oldEdgePostOpAgg",
                        "newEdgePostOpAgg",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                ),
                new MigrateElement(
                        MigrateElement.ElementType.EDGE,
                        "oldEdgeAggBeforePostFilter",
                        "newEdgeAggBeforePostFilter",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                ),
                new MigrateElement(
                        MigrateElement.ElementType.EDGE,
                        "edgeOldOpChain",
                        "edgeNewOpChain",
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToLong())
                                .project(COUNT)
                                .build(),
                        new ElementTransformer.Builder()
                                .select(COUNT)
                                .execute(new ToInteger())
                                .project(COUNT)
                                .build()
                )
        ));
        return migration;
    }
}
