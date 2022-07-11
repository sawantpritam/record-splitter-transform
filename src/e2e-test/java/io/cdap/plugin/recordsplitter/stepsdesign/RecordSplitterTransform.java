/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.recordsplitter.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.recordsplitter.actions.RecordSplitterActions;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * RecordSplitter Plugin related step design.
 */
public class RecordSplitterTransform implements CdfHelper {

  @Then("Validate OUT record count is equal to records transferred to target BigQuery table")
  public void validateOUTRecordCountIsEqualToRecordsTransferredToTargetBigQueryTable()
    throws IOException, InterruptedException, IOException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqTargetTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with target BigQuery table records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);
  }

  @Then("Validate OUT record count of record splitter is equal to IN record count of sink")
  public void validateOUTRecordCountOfRecordSplitterIsEqualToINRecordCountOfSink() {
    Assert.assertEquals(recordOut(), RecordSplitterActions.getTargetRecordsCount());
  }

  @Then("Enter Record Splitter plugin outputSchema {string}")
  public void enterRecordSplitterPluginOutputSchema(String jsonOutputSchema) {
    RecordSplitterActions.enterOutputSchema(jsonOutputSchema);
  }

  @Then("Select Record Splitter plugin output schema action: {string}")
  public void selectRecordSplitterPluginOutputSchemaAction(String action) {
    RecordSplitterActions.selectOutputSchemaAction(action);
  }
}
