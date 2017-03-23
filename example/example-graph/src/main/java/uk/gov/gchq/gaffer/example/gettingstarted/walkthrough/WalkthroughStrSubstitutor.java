/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.example.gettingstarted.walkthrough;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.example.gettingstarted.analytic.LoadAndQuery;
import uk.gov.gchq.gaffer.example.gettingstarted.function.aggregate.VisibilityAggregator;
import uk.gov.gchq.gaffer.example.gettingstarted.function.transform.MeanTransform;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator1;
import uk.gov.gchq.gaffer.example.gettingstarted.serialiser.VisibilitySerialiser;
import uk.gov.gchq.gaffer.example.util.JavaSourceUtil;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.aggregate.Sum;
import uk.gov.gchq.gaffer.function.filter.Exists;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.Export;
import uk.gov.gchq.gaffer.operation.impl.export.GetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

public abstract class WalkthroughStrSubstitutor {
    private static final String JAVA_DOC_URL_PREFIX = "http://gchq.github.io/Gaffer/";
    private static final String GITHUB_URL_PREFIX = "https://github.com/gchq/Gaffer/blob/master/";
    private static final String GITHUB_WIKI_URL_PREFIX = "https://github.com/gchq/Gaffer/wiki/";
    private static final String JAVA_SRC_PATH = "/src/main/java/";
    private static final String RESOURCES_SRC_PATH = "/src/main/resources/";
    private static final String EXAMPLE_GRAPH_MODULE_PATH = "example/example-graph";
    private static final String EXAMPLE_RESOURCE_PATH = "example/gettingstarted";

    public static String substitute(final String description, final LoadAndQuery example, final int exampleId, final String header) {
        return substitute(description, createParameterMap(example, exampleId, header));
    }

    public static String substitute(final String text) {
        return substitute(text, createParameterMap());
    }

    public static String substitute(final String text, final Map<String, String> paramMap) {
        final String formattedDescription = new StrSubstitutor(paramMap).replace(text);
        final int startIndex = formattedDescription.indexOf("${");
        if (startIndex > -1) {
            final String tmp = formattedDescription.substring(startIndex + 2);
            final int endIndex = tmp.indexOf("}");
            if (endIndex > -1) {
                throw new RuntimeException("Parameter was not substituted: " + tmp.substring(0, endIndex));
            }
        }
        return formattedDescription;
    }

    public static Map<String, String> createParameterMap(final LoadAndQuery example, final int exampleId, final String header) {
        final Class<?> exampleClass = example.getClass();
        final Map<String, String> params = new HashMap<>();
        params.put("HEADER",
                "### " + header);
        params.put("CODE_LINK",
                "The code for this example is " + getGitHubCodeLink(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH));
        params.put("DATA",
                "\n```csv\n" + getResource("/" + EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/data.txt", exampleClass) + "\n```\n");
        params.put("DATA_GENERATOR_JAVA",
                JavaSourceUtil.getJava(DataGenerator1.class.getName().replace("1", String.valueOf(exampleId)), EXAMPLE_GRAPH_MODULE_PATH));
        params.put("STORE_PROPERTIES",
                "\n```properties\n" + getResource("/" + EXAMPLE_RESOURCE_PATH + "/mockaccumulostore.properties", exampleClass).replaceAll("#.*\\n", "") + "\n```\n");
        params.put("DATA_SCHEMA_LINK",
                getGitHubResourcesLink(EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/dataSchema.json", EXAMPLE_GRAPH_MODULE_PATH));
        params.put("DATA_TYPES_LINK",
                getGitHubResourcesLink(EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/dataTypes.json", EXAMPLE_GRAPH_MODULE_PATH));
        params.put("STORE_TYPES_LINK",
                getGitHubResourcesLink(EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/storeTypes.json", EXAMPLE_GRAPH_MODULE_PATH));
        params.put("STORE_PROPERTIES_LINK",
                getGitHubResourcesLink(EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/mockaccumulostore.properties", EXAMPLE_GRAPH_MODULE_PATH));
        params.put("DATA_SCHEMA_JSON",
                "\n```json\n" + getResource("/" + EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/dataSchema.json", exampleClass) + "\n```\n");
        params.put("DATA_TYPES_JSON",
                "\n```json\n" + getResource("/" + EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/dataTypes.json", exampleClass) + "\n```\n");
        params.put("STORE_TYPES_JSON",
                "\n```json\n" + getResource("/" + EXAMPLE_RESOURCE_PATH + "/" + exampleId + "/schema/storeTypes.json", exampleClass) + "\n```\n");
        params.put("USER_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "user"));
        params.put("GENERATE_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "generate"));
        params.put("GRAPH_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "graph"));
        params.put("ADD_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "add"));
        params.put("TRANSFORM_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "transform"));
        params.put("GET_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get"));
        params.put("GET_PUBLIC_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get public"));
        params.put("GET_PRIVATE_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get private"));
        params.put("EXTRACTOR_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "extractor"));
        params.put("GET_ALL_EDGES_SUMMARISED_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get all edges summarised"));
        params.put("GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get all edges summarised in time window"));
        params.put("GET_ALL_CARDINALITIES_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get all cardinalities"));
        params.put("GET_ALL_SUMMARISED_CARDINALITIES_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get all summarised cardinalities"));
        params.put("GET_RED_EDGE_CARDINALITY_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get red edge cardinality 1"));
        params.put("GET_FREQUENCIES_OF_1_AND_9_FOR_EDGE_A_B_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get frequencies of 1L and 9L"));
        params.put("GET_0.25_0.5_0.75_PERCENTILES_FOR_EDGE_A_B_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get 0.25, 0.5, 0.75 percentiles"));
        params.put("GET_CDF_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get cdf"));
        params.put("GET_SAMPLE_FOR_EDGE_A_B_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get strings sample from the red edge"));
        params.put("GET_ENTITY_FOR_X_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get sample from the blue entity"));
        params.put("GET_ESTIMATE_SEPARATE_DAYS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get estimate separate days"));
        params.put("INTERSECT_ACROSS_DAYS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get intersection"));
        params.put("UNION_ACROSS_DAYS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get union across all days"));
        params.put("JOB_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "job"));
        params.put("EXECUTE_JOB_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "execute job"));
        params.put("JOB_DETAILS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "job details"));
        params.put("ALL_JOB_DETAILS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "all job details"));
        params.put("GET_JOB_RESULTS_SNIPPET",
                JavaSourceUtil.getJavaSnippet(example.getClass(), EXAMPLE_GRAPH_MODULE_PATH, "get job results"));
        params.put("RESULT_CACHE_EXPORT_OPERATIONS",
                "\n```json\n" + getResource("ResultCacheExportOperations.json", exampleClass).replaceAll("#.*\\n", "") + "\n```\n");
        params.put("CACHE_STORE_PROPERTIES",
                "\n```\n" + getResource("cache-store.properties", exampleClass).replaceAll("#.*\\n", "") + "\n```\n");
        try {
            example.run();
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        for (final Entry<String, StringBuilder> log : example.getLogCache().entrySet()) {
            params.put(log.getKey(), log.getValue().toString() + "\n");
        }

        params.putAll(createParameterMap());
        return params;
    }

    public static Map<String, String> createParameterMap() {
        final Map<String, String> params = new HashMap<>();
        params.put("EDGE_JAVADOC", getJavaDocLink(Edge.class));
        params.put("STORE_JAVADOC", getJavaDocLink(Store.class));
        params.put("ACCUMULO_STORE_JAVADOC", getJavaDocLink(AccumuloStore.class));
        params.put("MOCK_ACCUMULO_STORE_JAVADOC", getJavaDocLink(MockAccumuloStore.class));
        params.put("GRAPH_JAVADOC", getJavaDocLink(Graph.class));
        params.put("ELEMENT_GENERATOR_JAVADOC", getJavaDocLink(ElementGenerator.class));
        params.put("ELEMENT_JAVADOC", getJavaDocLink(Element.class));
        params.put("SCHEMA_JAVADOC", getJavaDocLink(Schema.class));
        params.put("PROPERTIES_JAVADOC", getJavaDocLink(Properties.class));
        params.put("ADD_ELEMENTS_JAVADOC", getJavaDocLink(AddElements.class));
        params.put("OPERATION_JAVADOC", getJavaDocLink(Operation.class));
        params.put("GET_RELATED_EDGES_JAVADOC", getJavaDocLink(GetEdges.class));
        params.put("VIEW_JAVADOC", getJavaDocLink(View.class));
        params.put("SUM_JAVADOC", getJavaDocLink(Sum.class));
        params.put("EXISTS_JAVADOC", getJavaDocLink(Exists.class));
        params.put("VIEW_ELEMENT_DEF_JAVADOC", getJavaDocLink(ViewElementDefinition.class));
        params.put("FILTER_FUNCTION_JAVADOC", getJavaDocLink(FilterFunction.class));
        params.put("ELEMENT_TRANSFORMER_JAVADOC", getJavaDocLink(ElementTransformer.class));
        params.put("FUNCTION_JAVADOC", getJavaDocLink(Function.class));
        params.put("TRANSFORM_FUNCTION_JAVADOC", getJavaDocLink(TransformFunction.class));
        params.put("GET_ADJACENT_ENTITY_SEEDS_JAVADOC", getJavaDocLink(GetAdjacentEntitySeeds.class));
        params.put("GENERATE_OBJECTS_JAVADOC", getJavaDocLink(GenerateObjects.class));
        params.put("ENTITY_SEED_EXTRACTOR_JAVADOC", getJavaDocLink(EntitySeedExtractor.class));
        params.put("FETCH_EXPORT_JAVADOC", getJavaDocLink(GetExport.class));
        params.put("UPDATE_EXPORT_JAVADOC", getJavaDocLink(Export.class));

        params.put("EXAMPLES_LINK", getGitHubPackageLink("Examples", LoadAndQuery.class.getPackage().getName(), EXAMPLE_GRAPH_MODULE_PATH));

        params.put("MEAN_TRANSFORM_LINK", getGitHubCodeLink(MeanTransform.class, EXAMPLE_GRAPH_MODULE_PATH));
        params.put("VISIBILITY_AGGREGATOR_LINK", getGitHubCodeLink(VisibilityAggregator.class, EXAMPLE_GRAPH_MODULE_PATH));
        params.put("VISIBILITY_SERIALISER_LINK", getGitHubCodeLink(VisibilitySerialiser.class, EXAMPLE_GRAPH_MODULE_PATH));
        params.put("ACCUMULO_USER_GUIDE", "[Accumulo Store User Guide](https://github.com/gchq/Gaffer/wiki/Accumulo-Store-User-Guide)");
        params.put("AGGREGATE_FUNCTION", getGitHubCodeLink(AggregateFunction.class, "core/function"));
        params.put("ACCUMULO_KEY_PACKAGE", getGitHubCodeLink(AccumuloKeyPackage.class, "store-implementations/accumulo-store"));


        params.put("OPERATION_EXAMPLES_LINK", getGitHubWikiLink("Operation Examples"));
        return params;
    }

    private static String getGitHubWikiLink(final String page) {
        return "[" + page + "](" + GITHUB_WIKI_URL_PREFIX + page.toLowerCase(Locale.getDefault()).replace(" ", "-") + ")";
    }

    private static String getResource(final String resourcePath, final Class<?> clazz) {
        final String resource;
        try (final InputStream stream = StreamUtil.openStream(clazz, resourcePath)) {
            if (null == stream) {
                resource = "";
            } else {
                resource = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return resource;
    }

    private static String getJavaDocLink(final Class<?> clazz) {
        return "[" + clazz.getSimpleName() + "](" + JAVA_DOC_URL_PREFIX + clazz.getName().replaceAll("\\.", "/") + ".html)";
    }

    private static String getGitHubResourcesLink(final String resourcePath, final String modulePath) {
        final String resourceName = resourcePath.substring(resourcePath.lastIndexOf("/") + 1, resourcePath.length());
        return "[" + resourceName + "](" + GITHUB_URL_PREFIX + modulePath + RESOURCES_SRC_PATH + resourcePath + ")";
    }

    private static String getGitHubPackageLink(final String displayName, final String packagePath, final String modulePath) {
        return "[" + displayName + "](" + GITHUB_URL_PREFIX + modulePath + JAVA_SRC_PATH + packagePath.replaceAll("\\.", "/") + ")";
    }

    private static String getGitHubCodeLink(final Class<?> clazz, final String modulePath) {
        return getGitHubCodeLink(clazz.getName(), modulePath);
    }

    private static String getGitHubCodeLink(final String className, final String modulePath) {
        final String simpleClassName = className.substring(className.lastIndexOf(".") + 1, className.length());
        return "[" + simpleClassName + "](" + GITHUB_URL_PREFIX + modulePath + JAVA_SRC_PATH + className.replaceAll("\\.", "/") + ".java)";
    }
}
