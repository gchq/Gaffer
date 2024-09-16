/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.rest.example.ExampleDomainObject;
import uk.gov.gchq.gaffer.rest.example.ExampleDomainObjectGenerator;
import uk.gov.gchq.gaffer.rest.example.ExampleElementGenerator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.First;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.impl.predicate.IsLongerThan;

import javax.annotation.PostConstruct;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BinaryOperator;

import static java.lang.reflect.Modifier.isStatic;

/**
 * Abstract Examples factory which can create examples using a schema and class inspection.
 */
@SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract") //Class is not particularly abstract
public abstract class AbstractExamplesFactory implements ExamplesFactory {
    private Map<Class<? extends Operation>, Operation> examplesMap;

    @PostConstruct
    public void generateExamples() {
        final Map<Class<? extends Operation>, Operation> map = new HashMap<>();
        map.put(GetAllElements.class, getAllElements());
        map.put(GetElements.class, getElements());
        map.put(GetAdjacentIds.class, getAdjacentIds());
        map.put(AddElements.class, addElements());
        map.put(GenerateObjects.class, generateObjects());
        map.put(GenerateElements.class, generateElements());
        map.put(OperationChain.class, operationChain());
        map.put(Sort.class, sort());
        map.put(Max.class, max());
        map.put(Min.class, min());
        map.put(ToMap.class, toMap());
        map.put(GetWalks.class, getWalks());
        map.put(AddNamedOperation.class, addNamedOperation());
        map.put(AddNamedView.class, addNamedView());
        map.put(If.class, ifOperation());
        map.put(While.class, whileOperation());
        map.put(uk.gov.gchq.gaffer.operation.impl.Map.class, mapOperation());

        examplesMap = map;
    }

    @Override
    @SuppressFBWarnings(value = "REFLC_REFLECTION_MAY_INCREASE_ACCESSIBILITY_OF_CLASS", justification = "Investigate")
    public Operation generateExample(final Class<? extends Operation> opClass) throws IllegalAccessException, InstantiationException {
        if (null == examplesMap) {
            generateExamples();
        }

        if (examplesMap.containsKey(opClass)) {
            return examplesMap.get(opClass);
        } else {
            final Operation operation = opClass.newInstance();
            final List<Field> fields = Arrays.asList(opClass.getDeclaredFields());

            for (final Field field : fields) {
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                if (!isStatic(field.getModifiers())) {
                    field.set(operation, getExampleValue(field.getType(), ThreadLocalRandom
                            .current().nextInt(0, 11)));
                }
            }
            return operation;
        }
    }

    private Object getExampleValue(final Class clazz, final int uniqueId) {
        Object value;
        try {
            if (null == clazz) {
                value = null;
            } else if (Character.class.equals(clazz) || char.class.equals(clazz)) {
                value = (char) uniqueId;
            } else if (String.class.equals(clazz) || Object.class.equals(clazz)) {
                value = String.valueOf(uniqueId);
            } else if (Integer.class.equals(clazz) || int.class.equals(clazz)) {
                value = uniqueId;
            } else if (Double.class.equals(clazz) || double.class.equals(clazz)) {
                value = (double) uniqueId;
            } else if (Long.class.equals(clazz) || long.class.equals(clazz)) {
                value = (long) uniqueId;
            } else if (Float.class.equals(clazz) || float.class.equals(clazz)) {
                value = (float) uniqueId;
            } else if (Date.class.equals(clazz)) {
                value = new Date(System.currentTimeMillis() - 10000 + uniqueId);
            } else if (boolean.class.equals(clazz)) {
                value = uniqueId % 2 == 0;
            } else if (BinaryOperator.class.equals(clazz)) {
                value = new First();
            } else if (clazz.isEnum()) {
                final List<Object> l = Arrays.asList(clazz.getEnumConstants());
                if (!l.isEmpty()) {
                    value = Enum.valueOf(clazz, l.get(0).toString());
                } else {
                    value = clazz.getDeclaredConstructor().newInstance();
                }
            } else {
                value = clazz.getDeclaredConstructor().newInstance();
            }

        } catch (final InstantiationException |
                       IllegalAccessException |
                       NoSuchMethodException |
                       InvocationTargetException e) {
            value = null;
        }


        return value;
    }

    protected abstract Schema getSchema();

    protected Entity getEntity(final int uniqueId) {
        final String group = getAnEntityGroup();
        final SchemaElementDefinition entityDef = getSchema().getEntity(group);

        final Entity entity = new Entity.Builder()
                .group(group)
                .vertex(getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), uniqueId))
                .build();
        populateProperties(entity, entityDef, uniqueId);

        return entity;
    }

    protected Edge getEdge(final int uniqueId1, final int uniqueId2) {
        final String group = getAnEdgeGroup();
        final SchemaElementDefinition edgeDef = getSchema().getEdge(group);

        final Edge edge = new Edge.Builder()
                .group(group)
                .source(getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), uniqueId1))
                .dest(getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.DESTINATION), uniqueId2))
                .directed(isEdgeDirected(group))
                .build();

        populateProperties(edge, edgeDef, uniqueId1);

        return edge;
    }

    protected EntityId getEntityId(final int uniqueId) {
        return new EntitySeed(
                getExampleVertex(getSchema().getEntity(getAnEntityGroup())
                        .getIdentifierClass(IdentifierType.VERTEX), uniqueId));
    }

    protected EdgeId getEdgeId(final int uniqueId1, final int uniqueId2) {
        final String group = getAnEdgeGroup();
        final SchemaEdgeDefinition edge = getSchema().getEdge(group);
        final Object sourceVert = getExampleVertex(edge.getIdentifierClass(IdentifierType.SOURCE), uniqueId1);
        final Object destVert = getExampleVertex(edge.getIdentifierClass(IdentifierType.DESTINATION), uniqueId2);

        if (edge.getDirected() == null) {
            return new EdgeSeed(sourceVert, destVert);
        }

        return new EdgeSeed(sourceVert, destVert, isEdgeDirected(group));
    }

    protected boolean isEdgeDirected(final String group) {
        SchemaEdgeDefinition edge = getSchema().getEdge(group);
        // Schema may not define if directed
        if (edge.getDirected() == null) {
            return false;
        }

        return edge.getDirected()
            .toLowerCase(Locale.getDefault())
            .contains("true");
    }

    protected String getAnEntityPropertyName() {
        final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
        String propertyName = null;
        if (null != entityDef && !entityDef.getProperties().isEmpty()) {
            propertyName = entityDef.getProperties().iterator().next();
        }

        return propertyName;
    }

    protected String getAnEntityGroup() {
        if (getSchema().getEntityGroups().isEmpty()) {
            return "exampleEntityGroup";
        }

        for (final Map.Entry<String, SchemaEntityDefinition> entry : getSchema()
                .getEntities()
                .entrySet()) {
            // Try and find an entity that has properties
            if (null != entry.getValue()
                    .getProperties() && !entry.getValue()
                    .getProperties()
                    .isEmpty()) {
                return entry.getKey();
            }
        }
        // if no entities have properties just return the first entity.
        return getSchema().getEntityGroups().iterator().next();
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
        if (getSchema().getEdgeGroups().isEmpty()) {
            return "exampleEdgeGroup";
        }

        for (final Map.Entry<String, SchemaEdgeDefinition> entry : getSchema().getEdges()
                .entrySet()) {
            // Try and find an edge that has properties
            if (null != entry.getValue()
                    .getProperties() && !entry.getValue()
                    .getProperties()
                    .isEmpty()) {
                return entry.getKey();
            }
        }
        // if no edges have properties just return the first entity.
        return getSchema().getEdgeGroups().iterator().next();

    }

    protected boolean hasEdges() {
        return !getSchema().getEdges().isEmpty();
    }

    protected boolean hasEntities() {
        return !getSchema().getEntities().isEmpty();
    }

    protected void populateProperties(final Element element, final SchemaElementDefinition elementDef, final int uniqueId) {
        for (final String property : elementDef.getProperties()) {
            element.putProperty(property, getExampleValue(elementDef.getPropertyClass(property), uniqueId));
        }
    }

    protected Object getExampleVertex(final Class<?> clazz, final int uniqueId) {
        if (String.class.equals(clazz) || Object.class.equals(clazz)) {
            return "vertex" + uniqueId;
        }

        return getExampleValue(clazz, uniqueId);
    }

    public GetAdjacentIds getAdjacentIds() {
        final GetAdjacentIds op = new GetAdjacentIds();
        final List<EntityId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }

    public GetAllElements getAllElements() {
        final GetAllElements op = new GetAllElements();
        populateOperation(op);
        return op;
    }

    public GetElements getElements() {
        final GetElements op = new GetElements();
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }

        if (hasEdges()) {
            seeds.add(getEdgeId(1, 2));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }

    public AddElements addElements() {
        final AddElements op = new AddElements();
        final List<Element> elements = new ArrayList<>();
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

    public GenerateObjects generateObjects() {
        final GenerateObjects<ExampleDomainObject> op = new GenerateObjects<>(new ExampleDomainObjectGenerator());
        final List<Element> elements = new ArrayList<>();
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

    public GenerateElements generateElements() {
        final GenerateElements<ExampleDomainObject> op = new GenerateElements<>(new ExampleElementGenerator());
        final ArrayList<ExampleDomainObject> objs = new ArrayList<>();
        if (hasEntities()) {
            final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 1)));
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 2)));
        }


        if (hasEdges()) {
            final String group = getAnEdgeGroup();
            final SchemaElementDefinition edgeDef = getSchema().getEdge(group);
            objs.add(new ExampleDomainObject(group,
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), 1),
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.DESTINATION), 1),
                    isEdgeDirected(group)));
        }

        op.setInput(objs);
        populateOperation(op);
        return op;
    }

    public Sort sort() {
        return new Sort.Builder()
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(getAnEdgeGroup())
                        .property(getAnEntityPropertyName())
                        .reverse(true)
                        .build())
                .resultLimit(20)
                .deduplicate(true)
                .build();
    }

    public Max max() {
        return new Max.Builder()
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(getAnEdgeGroup())
                        .property(getAnEdgePropertyName())
                        .build())
                .build();
    }

    public Min min() {
        return new Min.Builder()
                .comparators(new ElementPropertyComparator.Builder()
                        .groups(getAnEdgeGroup())
                        .property(getAnEdgePropertyName())
                        .build())
                .build();
    }

    public ToMap toMap() {
        return new ToMap.Builder()
                .generator(new MapGenerator.Builder()
                        .group(getAnEdgeGroup())
                        .source("source")
                        .property(getAnEdgePropertyName(), "edge property " + getAnEdgePropertyName())
                        .build())
                .build();
    }

    public GetWalks getWalks() {
        final List<String> edges = new ArrayList<>(getSchema().getEdgeGroups());
        if (edges.isEmpty() || getSchema().getEntityGroups().isEmpty()) {
            return new GetWalks();
        }

        final EntityId entityId = getEntityId(1);
        if (null == entityId.getVertex()) {
            entityId.setVertex("vertex1");
        }

        return new GetWalks.Builder()
                .input(entityId)
                .operations(new GetElements.Builder()
                        .view(new View.Builder()
                                .edge(edges.size() > 1 ? edges.get(1) : edges.get(0))
                                .build())
                        .build())
                .resultsLimit(10000)
                .build();
    }

    public AddNamedOperation addNamedOperation() {
        return new AddNamedOperation.Builder()
                .name("My Named Operation")
                .labels(Arrays.asList("Optional list of group labels"))
                .description("Optional description of my named operation")
                .score(2)
                .operationChain(new OperationChain.Builder().first(new GetAllElements()).build())
                .overwrite(true)
                .options(new HashMap<>())
                .build();
    }

    public AddNamedView addNamedView() {
        return new AddNamedView.Builder()
                .name("summarise")
                .description("Summarises all elements")
                .overwrite(true)
                .view(new View.Builder()
                        .globalElements(new GlobalViewElementDefinition.Builder()
                                .groupBy()
                                .build())
                        .build())
                .build();
    }

    public If ifOperation() {
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }
        return new If.Builder<>()
                .input(seeds)
                .conditional(new IsLongerThan(0))
                .then(new GetElements())
                .otherwise(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Limit<>(10))
                        .build())
                .build();
    }

    public While whileOperation() {
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }
        return new While.Builder<>()
                .input(seeds)
                .conditional(new IsLongerThan(0))
                .operation(new GetAdjacentIds())
                .maxRepeats(10)
                .build();
    }

    public uk.gov.gchq.gaffer.operation.impl.Map mapOperation() {
        return new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>()
                .input(2)
                .first(new ToString())
                .build();
    }

    public OperationChain operationChain() {
        return new OperationChain.Builder()
                .first(getAllElements())
                .then(new Limit<>(1))
                .build();
    }

    private void populateOperation(final Output operation) {
        populateOperation((Operation) operation);
    }

    protected void populateOperation(final Operation operation) {
        // override to add options to the operation
    }
}
