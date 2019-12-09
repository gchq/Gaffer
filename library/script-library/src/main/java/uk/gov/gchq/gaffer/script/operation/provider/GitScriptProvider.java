/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.script.operation.provider;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class GitScriptProvider implements ScriptProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(GitScriptProvider.class);

    /**
     * Gets the scripts from the given GIT repo URI and places
     * them at the given path
     *
     * @param absoluteRepoPath       the path to clone the repo to
     * @param repoURI                the URI of the GIT repo with the scripts
     */
    @Override
    public void retrieveScripts(final String absoluteRepoPath,
                                final String repoURI) throws IOException {
        if (absoluteRepoPath == null) {
            Git git = Git.open(new File(absoluteRepoPath));
            pullRepo(git);
        } else {
            cloneRepo(absoluteRepoPath,repoURI);
        }
    }

    /**
     * Pull the files using GIT
     *
     * @param git                    the git client
     */
    private synchronized void pullRepo(final Git git) {
        try {
            LOGGER.info("Repo already cloned, pulling files...");
            git.pull().call();
            LOGGER.info("Pulled the latest files.");
        } catch (final GitAPIException e) {
            e.printStackTrace();
        }
    }

    /**
     * Clone the files using GIT
     *
     * @param absoluteRepoPath       the path to clone the repo to
     * @param repoURI                the URI of the GIT repo with the scripts
     */
    private synchronized void cloneRepo(final String absoluteRepoPath, final String repoURI) {
        try {
            LOGGER.info("Cloning repo...");
            Git.cloneRepository().setDirectory(new File(absoluteRepoPath)).setURI(repoURI).call();
            LOGGER.info("Cloned the repo");
        } catch (final GitAPIException e) {
            e.printStackTrace();
        }
    }
}
