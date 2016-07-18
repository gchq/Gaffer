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

package gaffer.rest.service;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.GetOperation;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllEdges;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetAllEntities;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetElementsBySeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.rest.GraphFactory;
import gaffer.rest.example.ExampleDomainObject;
import gaffer.rest.example.ExampleDomainObjectGenerator;
import gaffer.rest.example.ExampleFilterFunction;
import gaffer.rest.example.ExampleTransformFunction;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaElementDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;


public class SimpleExamplesService implements IExamplesService {
    public static final String TRANSFORMED_PROPERTIES = "transformedProperties";
    private final GraphFactory graphFactory;

    public SimpleExamplesService() {
        this(GraphFactory.createGraphFactory());
    }

    public SimpleExamplesService(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public OperationChain execute() {
        final AddElements addElements = addElements();
        // delete the example elements as these are generated from the generate elements op
        addElements.setElements(null);
        return new OperationChain.Builder()
                .first(generateElements())
                .then(addElements)
                .build();
    }

    @Override
    public GetElementsBySeed<ElementSeed, Element> getElementsBySeed() {
        final GetElementsBySeed<ElementSeed, Element> op = new GetElementsBySeed<>();
        final List<ElementSeed> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntitySeed(1));
        }

        if (hasEdges()) {
            seeds.add(getEdgeSeed(1, 2));
        }

        op.setSeeds(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetRelatedElements<ElementSeed, Element> getRelatedElements() {
        final GetRelatedElements<ElementSeed, Element> op = new GetRelatedElements<>();
        final List<ElementSeed> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntitySeed(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeSeed(1, 2).getSource()));
        }

        if (hasEdges()) {
            seeds.add(getEdgeSeed(1, 2));
        }

        op.setSeeds(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetEntitiesBySeed getEntitiesBySeed() {
        final GetEntitiesBySeed op = new GetEntitiesBySeed();
        if (hasEntities()) {
            op.setSeeds(Collections.singletonList(getEntitySeed(1)));
        }
        populateOperation(op);
        return op;
    }

    @Override
    public GetRelatedEntities getRelatedEntities() {
        final GetRelatedEntities op = new GetRelatedEntities();
        final List<ElementSeed> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntitySeed(1));
        }

        if (hasEdges()) {
            seeds.add(getEdgeSeed(1, 2));
        }

        op.setSeeds(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetEdgesBySeed getEdgesBySeed() {
        final GetEdgesBySeed op = new GetEdgesBySeed();
        if (hasEdges()) {
            op.setSeeds(Collections.singletonList(getEdgeSeed(1, 2)));
        }
        populateOperation(op);
        return op;
    }

    @Override
    public GetRelatedEdges getRelatedEdges() {
        final GetRelatedEdges op = new GetRelatedEdges();
        final List<ElementSeed> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntitySeed(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeSeed(1, 2).getSource()));
        }

        if (hasEdges()) {
            seeds.add(getEdgeSeed(1, 2));
        }

        op.setSeeds(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetAdjacentEntitySeeds getAdjacentEntitySeeds() {
        final GetAdjacentEntitySeeds op = new GetAdjacentEntitySeeds();
        final List<EntitySeed> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntitySeed(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeSeed(1, 2).getSource()));
        }

        op.setSeeds(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetAllElements<Element> getAllElements() {
        final GetAllElements<Element> op = new GetAllElements<>();
        populateOperation(op);
        return op;
    }

    @Override
    public GetAllEntities getAllEntities() {
        final GetAllEntities op = new GetAllEntities();
        populateOperation(op);
        return op;
    }

    @Override
    public GetAllEdges getAllEdges() {
        final GetAllEdges op = new GetAllEdges();
        populateOperation(op);
        return op;
    }

    @Override
    public AddElements addElements() {
        final AddElements op = new AddElements();
        List<Element> elements = new ArrayList<>();
        if (hasEntities()) {
            elements.add(getEntity(1));
            elements.add(getEntity(2));
        }
        if (hasEdges()) {
            elements.add(getEdge(1, 2));
        }

        op.setElements(elements);

        populateOperation(op);
        return op;
    }

    @Override
    public GenerateObjects generateObjects() {
        final GenerateObjects<Element, ExampleDomainObject> op = new GenerateObjects<>(new ExampleDomainObjectGenerator());
        List<Element> elements = new ArrayList<>();
        if (hasEntities()) {
            elements.add(getEntity(1));
            elements.add(getEntity(2));
        }
        if (hasEdges()) {
            elements.add(getEdge(1, 2));
        }

        op.setInput(elements);
        populateOperation(op);
        return op;
    }


    @Override
    public GenerateElements generateElements() {
        final GenerateElements<ExampleDomainObject> op = new GenerateElements<>(new ExampleDomainObjectGenerator());
        final ArrayList<ExampleDomainObject> objs = new ArrayList<>();
        if (hasEntities()) {
            final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 1)));
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 2)));
        }


        if (hasEdges()) {
            final SchemaElementDefinition edgeDef = getSchema().getEdge(getAnEdgeGroup());
            objs.add(new ExampleDomainObject(getAnEdgeGroup(),
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), 1),
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.DESTINATION), 1),
                    isAnEdgeDirected()));
        }

        op.setInput(objs);
        populateOperation(op);
        return op;
    }

    private Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    private void populateOperation(final GetOperation operation) {
        populateOperation((Operation) operation);

        View.Builder viewBuilder = generateViewBuilder();
        operation.setView(viewBuilder.build());
    }

    protected View.Builder generateViewBuilder() {
        final View.Builder viewBuilder = new View.Builder();
        if (hasEntities()) {
            viewBuilder.entity(getAnEntityGroup(), new ViewElementDefinition.Builder()
                    .transientProperty(TRANSFORMED_PROPERTIES, String.class)
                    .filter(new ElementFilter.Builder()
                            .select(getAnEntityPropertyName())
                            .execute(new ExampleFilterFunction())
                            .build())
                    .transformer(new ElementTransformer.Builder()
                            .select(getAnEntityPropertyName())
                            .execute(new ExampleTransformFunction())
                            .project(TRANSFORMED_PROPERTIES)
                            .build())
                    .build());
        }

        if (hasEdges()) {
            viewBuilder.edge(getAnEdgeGroup(), new ViewElementDefinition.Builder()
                    .transientProperty(TRANSFORMED_PROPERTIES, String.class)
                    .filter(new ElementFilter.Builder()
                            .select(getAnEdgePropertyName())
                            .execute(new ExampleFilterFunction())
                            .build())
                    .transformer(new ElementTransformer.Builder()
                            .select(getAnEdgePropertyName())
                            .execute(new ExampleTransformFunction())
                            .project(TRANSFORMED_PROPERTIES)
                            .build())
                    .build());
        }

        return viewBuilder;
    }

    protected void populateOperation(final Operation operation) {
        // override to add options to the operation
    }

    protected Entity getEntity(final int uniqueId) {
        final String group = getAnEntityGroup();
        final SchemaElementDefinition entityDef = getSchema().getEntity(group);

        final Entity entity = new Entity(group);
        entity.setVertex(getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), uniqueId));
        populateProperties(entity, entityDef, uniqueId);

        return entity;
    }

    protected Edge getEdge(final int uniqueId1, final int uniqueId2) {
        final String group = getAnEdgeGroup();
        final SchemaElementDefinition edgeDef = getSchema().getEdge(group);

        final Edge edge = new Edge(group);
        edge.setSource(getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), uniqueId1));
        edge.setDestination(getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.DESTINATION), uniqueId2));
        edge.setDirected(isAnEdgeDirected());

        populateProperties(edge, edgeDef, uniqueId1);

        return edge;
    }

    protected EntitySeed getEntitySeed(final int uniqueId) {
        return new EntitySeed(
                getExampleVertex(getSchema().getEntity(getAnEntityGroup()).getIdentifierClass(IdentifierType.VERTEX), uniqueId));
    }

    protected EdgeSeed getEdgeSeed(final int uniqueId1, final int uniqueId2) {
        return new EdgeSeed(
                getExampleVertex(getSchema().getEdge(getAnEdgeGroup()).getIdentifierClass(IdentifierType.SOURCE), uniqueId1),
                getExampleVertex(getSchema().getEdge(getAnEdgeGroup()).getIdentifierClass(IdentifierType.DESTINATION), uniqueId2),
                isAnEdgeDirected());
    }

    protected boolean isAnEdgeDirected() {
        return !getSchema().getEdge(getAnEdgeGroup()).getDirected().toLowerCase(Locale.getDefault()).contains("false");
    }

    protected String getAnEntityPropertyName() {
        final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
        final String propertyName;
        if (null != entityDef && !entityDef.getProperties().isEmpty()) {
            propertyName = entityDef.getProperties().iterator().next();
        } else {
            propertyName = "examplePropertyName";
        }

        return propertyName;
    }

    protected String getAnEntityGroup() {
        if (!getSchema().getEntityGroups().isEmpty()) {
            return getSchema().getEntityGroups().iterator().next();
        } else {
            return "exampleEntityGroup";
        }
    }

    protected String getAnEdgePropertyName() {
        final SchemaElementDefinition edgeDef = getSchema().getEdge(getAnEdgeGroup());
        final String propertyName;
        if (null != edgeDef && !edgeDef.getProperties().isEmpty()) {
            propertyName = edgeDef.getProperties().iterator().next();
        } else {
            propertyName = "examplePropertyName";
        }

        return propertyName;
    }

    protected String getAnEdgeGroup() {
        if (!getSchema().getEdgeGroups().isEmpty()) {
            return getSchema().getEdgeGroups().iterator().next();
        } else {
            return "exampleEdgeGroup";
        }
    }

    protected boolean hasEdges() {
        return !getSchema().getEdges().isEmpty();
    }

    protected boolean hasEntities() {
        return !getSchema().getEntities().isEmpty();
    }

    protected void populateProperties(final Element element, final SchemaElementDefinition elementDef, final int uniqueId) {
        for (String property : elementDef.getProperties()) {
            element.putProperty(property, getExampleValue(elementDef.getPropertyClass(property), uniqueId));
        }
    }

    protected Object getExampleVertex(final Class<?> clazz, final int uniqueId) {
        if (String.class.equals(clazz) || Object.class.equals(clazz)) {
            return "vertex" + uniqueId;
        }

        return getExampleValue(clazz, uniqueId);
    }

    protected Object getExampleValue(final Class clazz, final int uniqueId) {
        Object value;
        if (null == clazz) {
            value = null;
        } else if (String.class.equals(clazz) || Object.class.equals(clazz)) {
            value = String.valueOf(uniqueId);
        } else if (Integer.class.equals(clazz)) {
            value = uniqueId;
        } else if (Double.class.equals(clazz)) {
            value = (double) uniqueId + 0.1;
        } else if (Long.class.equals(clazz)) {
            value = (long) uniqueId;
        } else if (Float.class.equals(clazz)) {
            value = (float) uniqueId;
        } else if (Date.class.equals(clazz)) {
            value = new Date(System.currentTimeMillis() - 10000 + uniqueId);
        } else {
            try {
                if (clazz.isEnum()) {
                    List l = Arrays.asList(clazz.getEnumConstants());
                    if (l.size() > 0) {
                        value = Enum.valueOf(clazz, l.get(0).toString());
                    } else {
                        value = clazz.newInstance();
                    }
                } else {
                    value = clazz.newInstance();
                }
            } catch (InstantiationException | IllegalAccessException e) {
                value = null;
            }
        }

        return value;
    }
}
