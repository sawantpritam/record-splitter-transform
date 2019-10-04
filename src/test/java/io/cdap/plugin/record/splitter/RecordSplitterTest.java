/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.record.splitter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests {@link RecordSplitter}.
 */
public class RecordSplitterTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
  private static final Schema OUTPUT =
    Schema.recordOf("output",
                    Schema.Field.of("c", Schema.of(Schema.Type.STRING)));
  public static final RecordSplitterConfig VALID_CONFIG = new RecordSplitterConfig(
    "a",
    "\n",
    "c",
    OUTPUT.toString()
  );

  @Test
  public void testSplitter() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform = new RecordSplitter(VALID_CONFIG);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap\nrocks").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
    Assert.assertEquals("cdap", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("rocks", emitter.getEmitted().get(1).get("c"));
  }

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, INPUT);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateIncorrectSchema() {
    RecordSplitterConfig config = RecordSplitterConfig.builder(VALID_CONFIG)
      .setSchema("test")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RecordSplitterConfig.SCHEMA));

    config.validate(failureCollector, INPUT);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateIncorrectOutputField() {
    RecordSplitterConfig config = RecordSplitterConfig.builder(VALID_CONFIG)
      .setOutputField("d")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RecordSplitterConfig.OUTPUT_FIELD));

    config.validate(failureCollector, INPUT);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateFieldToSplitMissingInInputSchema() {
    RecordSplitterConfig config = RecordSplitterConfig.builder(VALID_CONFIG)
      .setFieldToSplit("z")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RecordSplitterConfig.FIELD_TO_SPLIT));

    config.validate(failureCollector, INPUT);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateIncorrectFieldToSplit() {
    RecordSplitterConfig config = RecordSplitterConfig.builder(VALID_CONFIG)
      .setFieldToSplit("b")
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RecordSplitterConfig.FIELD_TO_SPLIT));

    config.validate(failureCollector, INPUT);
    assertValidationFailed(failureCollector, paramName);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
