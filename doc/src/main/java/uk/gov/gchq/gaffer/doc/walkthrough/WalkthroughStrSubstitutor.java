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
package uk.gov.gchq.gaffer.doc.walkthrough;

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
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.doc.util.JavaSourceUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public abstract class WalkthroughStrSubstitutor {
    public static final String JAVA_DOC_URL_PREFIX = "http://gchq.github.io/Gaffer/";
    public static final String GITHUB_URL_PREFIX = "https://github.com/gchq/Gaffer/blob/master/";
    public static final String GITHUB_WIKI_URL_PREFIX = "https://github.com/gchq/Gaffer/wiki/";
    public static final String JAVA_SRC_PATH = "/src/main/java/";
    public static final String RESOURCES_SRC_PATH = "/src/main/resources/";

    public static String substitute(final String walkthrough, final AbstractWalkthrough example, final String modulePath, final String header, final String dataPath, final String schemaPath, final Class<? extends ElementGenerator> elementGenerator) {
        return substitute(walkthrough, createParameterMap(walkthrough, example, modulePath, header, dataPath, schemaPath, elementGenerator));
    }

    public static String substitute(final String walkthrough, final AbstractWalkthrough example, final String modulePath) {
        return substitute(walkthrough, createParameterMap(walkthrough, example, modulePath));
    }

    public static String substitute(final String walkthrough, final String modulePath) {
        return substitute(walkthrough, null, modulePath);
    }

    public static String substitute(final String walkthrough, final Map<String, String> paramMap) {
        return new StrSubstitutor(paramMap).replace(walkthrough);
    }

    public static void validateSubstitution(final String walkthrough) {
        final int startIndex = walkthrough.indexOf("${");
        if (startIndex > -1) {
            final String tmp = walkthrough.substring(startIndex + 2);
            final int endIndex = tmp.indexOf("}");
            if (endIndex > -1) {
                throw new RuntimeException("Parameter was not substituted: " + tmp.substring(0, endIndex));
            }
        }
    }

    public static Map<String, String> createParameterMap(final String text, final AbstractWalkthrough example, final String modulePath, final String header, final String dataPath, final String schemaPath, final Class<? extends ElementGenerator> elementGenerator) {
        final Class<?> exampleClass = example.getClass();
        final Map<String, String> params = new HashMap<>();
        params.put("HEADER",
                "### " + header);
        params.put("CODE_LINK",
                "The code for this example is " + getGitHubCodeLink(example.getClass(), modulePath) + ".");
        if (null != dataPath) {
            params.put("DATA",
                    "\n```csv\n" + getResource(dataPath, exampleClass) + "\n```\n");
        }
        if (null != elementGenerator) {
            params.put("ELEMENT_GENERATOR_JAVA",
                    JavaSourceUtil.getJava(elementGenerator.getName(), modulePath));
        }
        params.put("STORE_PROPERTIES",
                "\n```properties\n" + getResource("/mockaccumulostore.properties", exampleClass).replaceAll("#.*\\n", "") + "\n```\n");
        params.put("DATA_SCHEMA_LINK",
                getGitHubResourcesLink(schemaPath + "/dataSchema.json", modulePath));
        params.put("DATA_TYPES_LINK",
                getGitHubResourcesLink(schemaPath + "/dataTypes.json", modulePath));
        params.put("STORE_TYPES_LINK",
                getGitHubResourcesLink(schemaPath + "/storeTypes.json", modulePath));
        params.put("STORE_PROPERTIES_LINK",
                getGitHubResourcesLink("/mockaccumulostore.properties", modulePath));
        if (null != schemaPath) {
            params.put("DATA_SCHEMA_JSON",
                    "\n```json\n" + getResource(schemaPath + "/dataSchema.json", exampleClass) + "\n```\n");
            params.put("DATA_TYPES_JSON",
                    "\n```json\n" + getResource(schemaPath + "/dataTypes.json", exampleClass) + "\n```\n");
            params.put("STORE_TYPES_JSON",
                    "\n```json\n" + getResource(schemaPath + "/storeTypes.json", exampleClass) + "\n```\n");
        }

        params.putAll(createParameterMap(text, example, modulePath));
        return params;
    }

    public static Map<String, String> createParameterMap(final String text, final AbstractWalkthrough example, final String modulePath) {
        final Map<String, String> params = new HashMap<>();
        params.put("EDGE_JAVADOC", getJavaDocLink(Edge.class));
        params.put("STORE_JAVADOC", getJavaDocLink(Store.class));
        params.put("ACCUMULO_STORE_JAVADOC", getJavaDocLink(AccumuloStore.class));
        params.put("MOCK_ACCUMULO_STORE_JAVADOC", getJavaDocLink(MockAccumuloStore.class));
        params.put("GRAPH_JAVADOC", getJavaDocLink(Graph.class));
        params.put("ELEMENT_GENERATOR_JAVADOC", getJavaDocLink(ElementGenerator.class));
        params.put("OBJECT_GENERATOR_JAVADOC", getJavaDocLink(ObjectGenerator.class));
        params.put("ELEMENT_JAVADOC", getJavaDocLink(Element.class));
        params.put("SCHEMA_JAVADOC", getJavaDocLink(Schema.class));
        params.put("PROPERTIES_JAVADOC", getJavaDocLink(Properties.class));
        params.put("ADD_ELEMENTS_JAVADOC", getJavaDocLink(AddElements.class));
        params.put("OPERATION_JAVADOC", getJavaDocLink(Operation.class));
        params.put("GET_ELEMENTS_JAVADOC", getJavaDocLink(GetElements.class));
        params.put("VIEW_JAVADOC", getJavaDocLink(View.class));
        params.put("SUM_JAVADOC", getJavaDocLink(Sum.class));
        params.put("EXISTS_JAVADOC", getJavaDocLink(Exists.class));
        params.put("VIEW_ELEMENT_DEF_JAVADOC", getJavaDocLink(ViewElementDefinition.class));
        params.put("ELEMENT_TRANSFORMER_JAVADOC", getJavaDocLink(ElementTransformer.class));
        params.put("FUNCTION_JAVADOC", getJavaDocLink(Function.class));
        params.put("GET_ADJACENT_ENTITY_SEEDS_JAVADOC", getJavaDocLink(GetAdjacentIds.class));
        params.put("GENERATE_OBJECTS_JAVADOC", getJavaDocLink(GenerateObjects.class));
        params.put("ENTITY_SEED_EXTRACTOR_JAVADOC", getJavaDocLink(EntityIdExtractor.class));
        params.put("FETCH_EXPORT_JAVADOC", getJavaDocLink(GetSetExport.class));
        params.put("EXPORT_TO_SET_JAVADOC", getJavaDocLink(ExportToSet.class));
        params.put("EXPORT_TO_GAFFER_RESULT_CACHE_JAVADOC", getJavaDocLink(ExportToGafferResultCache.class));
        params.put("EXAMPLES_LINK", getGitHubPackageLink("Examples", AbstractWalkthrough.class.getPackage().getName(), modulePath));
        params.put("ACCUMULO_USER_GUIDE", "[Accumulo Store User Guide](https://github.com/gchq/Gaffer/wiki/Accumulo-Store-User-Guide)");
        params.put("ACCUMULO_KEY_PACKAGE", getGitHubCodeLink(AccumuloKeyPackage.class, "store-implementations/accumulo-store"));


        params.put("OPERATION_EXAMPLES_LINK", getGitHubWikiLink("Operation Examples"));

        if (null != example) {
            try {
                example.run();
            } catch (final OperationException | IOException e) {
                throw new RuntimeException(e);
            }

            for (final Map.Entry<String, StringBuilder> log : example.getLogCache().entrySet()) {
                params.put(log.getKey(), log.getValue().toString() + "\n");
            }
        }

        int position = 0;
        final int length = text.length();
        while (position < length) {
            final int startIndex = text.indexOf("${", position);
            position = length;
            if (startIndex > -1) {
                final String tmp = text.substring(startIndex + 2);
                final int endIndex = tmp.indexOf("}");
                if (endIndex > -1) {
                    position = startIndex + endIndex + 3;
                    final String param = tmp.substring(0, endIndex);
                    if (param.endsWith("_SNIPPET")) {
                        if (null != example) {
                            final String textId = param.replace("_SNIPPET", "").replaceAll("_", " ").toLowerCase(Locale.getDefault());
                            params.put(param, JavaSourceUtil.getJavaSnippet(example.getClass(), modulePath, textId));
                        }
                    }
                }
            }
        }
        return params;
    }

    public static String getGitHubWikiLink(final String page) {
        return "[" + page + "](" + GITHUB_WIKI_URL_PREFIX + page.toLowerCase(Locale.getDefault()).replace(" ", "-") + ")";
    }

    public static String getResource(final String resourcePath, final Class<?> clazz) {
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

    public static String getJavaDocLink(final Class<?> clazz) {
        return "[" + clazz.getSimpleName() + "](" + JAVA_DOC_URL_PREFIX + clazz.getName().replaceAll("\\.", "/") + ".html)";
    }

    public static String getGitHubResourcesLink(final String resourcePath, final String modulePath) {
        final String resourceName = resourcePath.substring(resourcePath.lastIndexOf("/") + 1, resourcePath.length());
        return "[" + resourceName + "](" + GITHUB_URL_PREFIX + modulePath + RESOURCES_SRC_PATH + resourcePath + ")";
    }

    public static String getGitHubPackageLink(final String displayName, final String packagePath, final String modulePath) {
        return "[" + displayName + "](" + GITHUB_URL_PREFIX + modulePath + JAVA_SRC_PATH + packagePath.replaceAll("\\.", "/") + ")";
    }

    public static String getGitHubCodeLink(final Class<?> clazz, final String modulePath) {
        return getGitHubCodeLink(clazz.getName(), modulePath);
    }

    public static String getGitHubCodeLink(final String className, final String modulePath) {
        final String simpleClassName = className.substring(className.lastIndexOf(".") + 1, className.length());
        return "[" + simpleClassName + "](" + GITHUB_URL_PREFIX + modulePath + JAVA_SRC_PATH + className.replaceAll("\\.", "/") + ".java)";
    }

    public static String getGitHubFileLink(final String displayName, final String path) {
        return "[" + displayName + "](" + GITHUB_URL_PREFIX + path + ")";
    }
}
