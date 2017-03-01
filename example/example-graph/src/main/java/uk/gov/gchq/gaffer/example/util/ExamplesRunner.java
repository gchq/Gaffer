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
package uk.gov.gchq.gaffer.example.util;

import com.google.common.collect.Sets;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * This runner will run a suite of examples.
 */
public abstract class ExamplesRunner {
    public static final String EXAMPLE_DIVIDER = "\n\n";
    private static final Logger LOGGER = LoggerFactory.getLogger(ExamplesRunner.class);

    public void run(final Class<? extends Example> exampleParentClass, final Class<?> classForExample, final String type) throws Exception {
        printHeader();
        printEditWarning(type);
        printTableOfContents(exampleParentClass);

        final Set<? extends Class<?>> classes = Sets.newHashSet((Iterable) getSubClasses(classForExample));
        for (final Class<? extends Example> aClass : getSubClasses(exampleParentClass, getClass().getPackage().getName())) {
            final Example example = aClass.newInstance();
            classes.remove(example.getClassForExample());
            example.run();
            log(EXAMPLE_DIVIDER);
        }
    }

    protected void log(final String msg) {
        LOGGER.info(msg);
    }

    private void printHeader() {
        log("Copyright 2016-2017 Crown Copyright\n"
                + "\n"
                + "Licensed under the Apache License, Version 2.0 (the \"License\");\n"
                + "you may not use this file except in compliance with the License.\n"
                + "You may obtain a copy of the License at\n"
                + "\n"
                + "  http://www.apache.org/licenses/LICENSE-2.0\n"
                + "\n"
                + "Unless required by applicable law or agreed to in writing, software\n"
                + "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                + "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                + "See the License for the specific language governing permissions and\n"
                + "limitations under the License.\n"
                + "\n");
    }

    protected void printEditWarning(final String type) {
        log("_This page has been generated from code. To make any changes please update the " + type + " examples in the [example](https://github.com/gchq/Gaffer/tree/master/example/example-graph/src/main/java/uk/gov/gchq/gaffer/example) module, run it and replace the content of this page with the output._\n\n");
    }

    protected void printTableOfContents(final Class<? extends Example> exampleParentClass) throws InstantiationException, IllegalAccessException {
        int index = 1;
        for (final Class<? extends Example> aClass : getSubClasses(exampleParentClass, getClass().getPackage().getName())) {
            final String opClass = aClass.newInstance().getClassForExample().getSimpleName();
            log(index + ". [" + opClass + "](#" + opClass.toLowerCase(Locale.getDefault()) + "-example)");
            index++;
        }
        log("\n");
    }

    private static <CLASS> List<Class<? extends CLASS>> getSubClasses(final Class<CLASS> clazz) {
        return getSubClasses(clazz, null);
    }


    private static <CLASS> List<Class<? extends CLASS>> getSubClasses(final Class<CLASS> clazz, final String packageName) {
        final Set<URL> urls = new HashSet<>(ClasspathHelper.forPackage("gaffer"));

        final List<Class<? extends CLASS>> classes = new ArrayList<>(new Reflections(urls).getSubTypesOf(clazz));
        keepPublicConcreteClasses(classes);
        keepClassesInPackage(classes, packageName);
        Collections.sort(classes, new Comparator<Class>() {
            @Override
            public int compare(final Class class1, final Class class2) {
                return class1.getName().compareTo(class2.getName());
            }
        });

        return classes;
    }

    private static void keepClassesInPackage(final List classes, final String packageName) {
        if (null != classes && null != packageName) {
            final Iterator<Class> itr = classes.iterator();
            while (itr.hasNext()) {
                final Class clazz = itr.next();
                if (null != clazz) {
                    if (!clazz.getPackage().getName().equals(packageName)) {
                        itr.remove();
                    }
                }
            }
        }
    }

    private static void keepPublicConcreteClasses(final List classes) {
        if (null != classes) {
            final Iterator<Class> itr = classes.iterator();
            while (itr.hasNext()) {
                final Class clazz = itr.next();
                if (null != clazz) {
                    final int modifiers = clazz.getModifiers();
                    if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers)) {
                        itr.remove();
                    }
                }
            }
        }
    }
}
