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
package uk.gov.gchq.gaffer.example.gettingstarted.walkthrough;

import org.apache.commons.io.IOUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.example.gettingstarted.analytic.LoadAndQuery;
import java.io.IOException;
import java.io.InputStream;
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
 * This runner will run all getting started walkthroughs.
 */
public class WalkthroughRunner {
    public static final String EXAMPLE_DIVIDER = "\n\n";
    private static final Logger LOGGER = LoggerFactory.getLogger(WalkthroughRunner.class);
    private static final List<Class<? extends LoadAndQuery>> EXAMPLES = getSubClasses(LoadAndQuery.class);

    public static void main(final String[] args) throws Exception {
        new WalkthroughRunner().run();
    }

    public void run() throws Exception {
        printHeader();
        printTableOfContents();
        printIntro();
        for (final Class<? extends LoadAndQuery> aClass : EXAMPLES) {
            LOGGER.info(aClass.newInstance().walkthrough());
            LOGGER.info(EXAMPLE_DIVIDER);
        }
    }

    private void printIntro() {
        final String intro;
        try (final InputStream stream = StreamUtil.openStream(getClass(), "/example/gettingstarted/intro.md")) {
            intro = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info(WalkthroughStrSubstitutor.substitute(intro));
    }

    private void printHeader() {
        LOGGER.info("Copyright 2016-2017 Crown Copyright\n"
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
                + "_This page has been generated from code. To make any changes please update the getting started example walkthroughs in the [example](https://github.com/gchq/Gaffer/tree/master/example/example-graph/src/main/java/uk/gov/gchq/gaffer/example) module, run it and replace the content of this page with the output._\n\n");
    }

    private void printTableOfContents() throws InstantiationException, IllegalAccessException {
        int index = 1;
        LOGGER.info(index + ". [A *Very* Short Introduction to Gaffer](#a-very-short-introduction-to-gaffer)");
        index++;
        LOGGER.info(index + ". [Walkthroughs](#walkthroughs)");

        index = 1;
        for (final Class<? extends LoadAndQuery> aClass : EXAMPLES) {
            final String header = aClass.newInstance().getHeader();
            LOGGER.info("  " + index + ". [" + header + "](#" + header.toLowerCase(Locale.getDefault()).replace(" ", "-") + ")");
            index++;
        }
        LOGGER.info("\n");
    }

    private static <CLASS> List<Class<? extends CLASS>> getSubClasses(final Class<CLASS> clazz) {
        final Set<URL> urls = new HashSet<>(ClasspathHelper.forPackage("uk.gov.gchq.gaffer.example"));

        final List<Class<? extends CLASS>> classes = new ArrayList<>(new Reflections(urls).getSubTypesOf(clazz));
        keepPublicConcreteClasses(classes);
        Collections.sort(classes, new Comparator<Class>() {
            @Override
            public int compare(final Class class1, final Class class2) {
                final int class1Number = Integer.parseInt(class1.getName().replaceAll(clazz.getName(), ""));
                final int class2Number = Integer.parseInt(class2.getName().replaceAll(clazz.getName(), ""));
                return class1Number - class2Number;
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
