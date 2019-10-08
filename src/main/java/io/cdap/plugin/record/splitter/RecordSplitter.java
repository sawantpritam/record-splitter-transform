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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.util.List;

/**
 * Given a field and a delimiter, this splits it into multiple records.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("RecordSplitter")
@Description("Given a field and a delimiter, this splits it into multiple records.")
public final class RecordSplitter extends Transform<StructuredRecord, StructuredRecord> {
  private final RecordSplitterConfig config;

  // Output Schema associated with transform output.
  private Schema outSchema;
  List<Schema.Field> fields;

  @VisibleForTesting
  public RecordSplitter(RecordSplitterConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    Schema inputSchema = context.getInputSchema();
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    outSchema = config.getSchema();
    fields = null;
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    if (fields == null) {
      fields = in.getSchema().getFields();
    }
    Object valueToSplit = in.get(config.getFieldToSplit());
    if (valueToSplit != null) {
      String[] records = String.valueOf(valueToSplit).split(config.getDelimiter());
      for (String record : records) {
        StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
        for (Schema.Field field : fields) {
          String name = field.getName();
          if (outSchema.getField(name) != null && !name.equals(config.getFieldToSplit())) {
            builder.set(name, in.get(name));
          }
        }
        builder.set(config.getOutputField(), record.trim());
        emitter.emit(builder.build());
      }
    }
  }
}
