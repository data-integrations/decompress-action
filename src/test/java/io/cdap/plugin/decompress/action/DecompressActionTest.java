/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.decompress.action;

import io.cdap.cdap.etl.mock.action.MockActionContext;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DecompressAction}
 */
public class DecompressActionTest {
  private static final String GZIPPED_FILE_NAME = "example.json.gz";
  private static final String ZIPPED_FILE_NAME = "example.zip";
  private static final String TARRED_FILE_NAME = "example.tar";
  private static final String TARRED_GZIPPED_FILE_NAME = "example.tar.gz";
  private static final String UNGZIPPED_FILE_NAME = "example.json";

  private FileFilter filter = new FileFilter() {
    private final Pattern pattern = Pattern.compile("[^\\.].*\\.json");

    @Override
    public boolean accept(File pathname) {
      return pattern.matcher(pathname.getName()).matches();
    }
  };

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSingleGZippedFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL gzippedFile = classLoader.getResource(GZIPPED_FILE_NAME);
    File destFolder = temporaryFolder.newFolder();
    DecompressActionConfig config = new DecompressActionConfig(gzippedFile.getFile(),
                                                               destFolder.getPath() + "/" + UNGZIPPED_FILE_NAME,
                                                               null, "Compressed", false);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new DecompressAction(config).configurePipeline(configurer);
    new DecompressAction(config).run(new MockActionContext());
    assertEquals(1, destFolder.listFiles(filter).length);
    assertEquals(UNGZIPPED_FILE_NAME, destFolder.listFiles(filter)[0].getName());
  }

  @Test
  public void testSingleGZippedFileFolderOutput() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL gzippedFile = classLoader.getResource(GZIPPED_FILE_NAME);
    File destFolder = temporaryFolder.newFolder();
    DecompressActionConfig config = new DecompressActionConfig(gzippedFile.getFile(),
                                                               destFolder.getPath(),
                                                               null, "Compressed", false);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new DecompressAction(config).configurePipeline(configurer);
    new DecompressAction(config).run(new MockActionContext());
    assertEquals(1, destFolder.listFiles(filter).length);
    assertEquals(UNGZIPPED_FILE_NAME, destFolder.listFiles(filter)[0].getName());
  }

  @Test
  public void testSingleZippedFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL zippedFile = classLoader.getResource(ZIPPED_FILE_NAME);
    File destFolder = temporaryFolder.newFolder();
    DecompressActionConfig config = new DecompressActionConfig(zippedFile.getFile(),
                                                               destFolder.getPath(),
                                                               null, "Archived", false);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new DecompressAction(config).configurePipeline(configurer);
    new DecompressAction(config).run(new MockActionContext());
    assertEquals(2, new File(destFolder.getPath() + "/example").listFiles(filter).length);
  }

  @Test
  public void testSingleTarredFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL tarredFile = classLoader.getResource(TARRED_FILE_NAME);
    File destFolder = temporaryFolder.newFolder();
    DecompressActionConfig config = new DecompressActionConfig(tarredFile.getFile(),
                                                               destFolder.getPath(),
                                                               null, "Archived", false);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new DecompressAction(config).configurePipeline(configurer);
    new DecompressAction(config).run(new MockActionContext());
    assertEquals(2, new File(destFolder.getPath() + "/example").listFiles(filter).length);
  }

  @Test
  public void testSingleTarredGZippedFile() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL tarredGZippedFile = classLoader.getResource(TARRED_GZIPPED_FILE_NAME);
    File destFolder = temporaryFolder.newFolder();
    DecompressActionConfig config = new DecompressActionConfig(tarredGZippedFile.getFile(),
                                                               destFolder.getPath(),
                                                               null, "Archived then compressed", false);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new DecompressAction(config).configurePipeline(configurer);
    new DecompressAction(config).run(new MockActionContext());
    assertEquals(2, new File(destFolder.getPath() + "/example").listFiles(filter).length);
  }
}
