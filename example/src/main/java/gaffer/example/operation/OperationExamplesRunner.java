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
package gaffer.example.operation;

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
 * This runner will run all operation examples.
 */
public class OperationExamplesRunner {
    public static final String EXAMPLE_DIVIDER = "\n\n";
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationExamplesRunner.class);

    public static void main(final String[] args) throws Exception {
        new OperationExamplesRunner().run();
    }

    public void run() throws Exception {
        printHeader();
        printTableOfContents();
        for (Class<? extends OperationExample> aClass : getSubClasses(OperationExample.class)) {
            aClass.newInstance().run();
            LOGGER.info(EXAMPLE_DIVIDER);
        }
    }

    private void printHeader() {
        LOGGER.info("Copyright 2016 Crown Copyright\n"
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
                + "\n"
                + "_This page has been generated from code. To make any changes please updated the operation examples in the [example](https://github.com/GovernmentCommunicationsHeadquarters/Gaffer/tree/master/example/src/main/java/gaffer/example) module, run it and replace the content of this page with the output._\n\n");
    }

    private void printTableOfContents() throws InstantiationException, IllegalAccessException {
        int index = 1;
        for (Class<? extends OperationExample> aClass : getSubClasses(OperationExample.class)) {
            final String opClass = aClass.newInstance().getOperationClass().getSimpleName();
            LOGGER.info(index + ". [" + opClass + "](#" + opClass.toLowerCase(Locale.getDefault()) + "-example)");
            index++;
        }
        LOGGER.info("\n");
    }

    private static <CLASS> List<Class<? extends CLASS>> getSubClasses(final Class<CLASS> clazz) {
        final Set<URL> urls = new HashSet<>(ClasspathHelper.forPackage("gaffer.example"));

        final List<Class<? extends CLASS>> classes = new ArrayList<>(new Reflections(urls).getSubTypesOf(clazz));
        keepPublicConcreteClasses(classes);
        Collections.sort(classes, new Comparator<Class>() {
            @Override
            public int compare(final Class class1, final Class class2) {
                return class1.getName().compareTo(class2.getName());
            }
        });

        return classes;

    }

    private static void keepPublicConcreteClasses(final List classes) {
        if (null != classes) {
            final Iterator<Class> itr = classes.iterator();
            for (Class clazz = null; itr.hasNext(); clazz = itr.next()) {
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
