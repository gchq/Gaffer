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
import java.nio.file.Files;
import java.nio.file.Paths;

public class GitScriptProvider implements ScriptProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(GitScriptProvider.class);

    public static GitScriptProvider gitScriptProvider() {
        return new GitScriptProvider();
    }

    /**
     * Gets the scripts from the given GIT repo URI and places
     * them at the given path
     *
     * @param absoluteRepoPath       the path to clone the repo to
     * @param repoURI                the URI of the GIT repo with the scripts
     */
    @Override
    public void retrieveScripts(final String absoluteRepoPath,
                                final String repoURI) throws GitAPIException, IOException {
        if (Files.notExists(Paths.get(absoluteRepoPath))) {
            cloneRepo(absoluteRepoPath, repoURI);
        } else {
            try {
                Git git = Git.open(new File(absoluteRepoPath + "/" + repoURI.substring(repoURI.lastIndexOf("/") + 1)));
                pullRepo(git);
            } catch (final IOException e) {
                LOGGER.error("absoluteRepoPath: {}", absoluteRepoPath);
                LOGGER.error("repoURI: {}", repoURI);
                LOGGER.error(e.getMessage());
                LOGGER.error("Failed to get the latest scripts");
                throw e;
            }
        }
    }

    /**
     * Pull the files using GIT
     *
     * @param git                    the git client
     * @throws GitAPIException       if it fails to pull the repo
     */
    private synchronized void pullRepo(final Git git) throws GitAPIException {
        try {
            LOGGER.info("Repo already cloned, pulling files...");
            git.pull().call();
            LOGGER.info("Pulled the latest files.");
        } catch (final GitAPIException e) {
            LOGGER.error(e.getMessage());
            LOGGER.error("Failed to pull the latest files");
            throw e;
        }
    }

    /**
     * Clone the files using GIT
     *
     * @param absoluteRepoPath       the path to clone the repo to
     * @param repoURI                the URI of the GIT repo with the scripts
     * @throws GitAPIException       if it fails to clone the repo
     */
    private synchronized void cloneRepo(final String absoluteRepoPath, final String repoURI) throws GitAPIException {
        try {
            LOGGER.info("Cloning repo...");
            LOGGER.debug("path: {}", absoluteRepoPath);
            LOGGER.debug("path: {}", repoURI);
            Git.cloneRepository()
                    .setURI(repoURI)
                    .setDirectory(new File(absoluteRepoPath))
                    .call();
            LOGGER.info("Cloned the repo");
        } catch (final GitAPIException e) {
            LOGGER.error(e.toString());
            LOGGER.error("Failed to clone the repo");
            throw e;
        }
    }
}
