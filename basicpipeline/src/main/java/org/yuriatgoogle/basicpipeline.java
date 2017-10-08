/* Copyright 2016 Noovle Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.yuriatgoogle;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.function.DoubleFunction;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection; 
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;





public class basicpipeline {

    private static final Logger LOG = LoggerFactory.getLogger(basicpipeline.class);
    //Google Cloud settings
    public static String topicID = "test-topic"; //pubsub topic
    public static String projectId = "ymg-basic-pipeline"; //project ID
    public static String datasetId = "dataset"; //BigQuery dataset
    public static String tableId = "testTable"; //BigQuery table
    public static String bucketId = "gs://ymg-dataflow-bucket"; //staging bucket for dataflow
    private static final String tableSchema = "{\"fields\":[{\"type\":\"STRING\",\"name\":\"message\",\"mode\":\"NULLABLE\"}]}"; //schema for the BQ table

        
    //create table schema from Json
    private static TableSchema createTableSchema(String schema) throws IOException {
        return JacksonFactory.getDefaultInstance().fromString(schema, TableSchema.class);
    }


    public static void main(String[] args) {

    	// Setup Dataflow options
        DataflowPipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .create()
            .as(DataflowPipelineOptions.class);

        options.setRunner(DataflowPipelineRunner.class);
        options.setStreaming(true);
        options.setStagingLocation(bucketId);

        //BQ table setup
        TableSchema bqTableSchema;
        try {
            bqTableSchema = createTableSchema(tableSchema);
        } catch (IOException e){
            e.printStackTrace();
            return;
        }
        
        // Create a TableReference for the destination table
        TableReference tableReference = new TableReference();
        tableReference.setProjectId(projectId);
        tableReference.setDatasetId(datasetId);
        tableReference.setTableId(tableId);
        
        Pipeline p = Pipeline.create(options);

        // Read message from Pub/Sub
        PCollection<String> messages = null;
        messages = p.apply(PubsubIO.
            Read.named("Read message from PubSub")
            .topic("projects/" + projectId + "/topics/" + topicID))

        // Format tweets for BigQuery - convert string to table row
        .apply("Format for BigQuery", ParDo.of(new DoFn<String, TableRow>() {
            //@ProcessElement
            public void processElement(ProcessContext c) {
                c.output(new TableRow().set("message", c.element()));
                //TODO - replace column reference
            }
        }))
        
        // Write tweets to BigQuery
        .apply("Write to BigQuery", BigQueryIO
            .writeTableRows()
            .to(tableReference)
            .withSchema(bqTableSchema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );

        //run pipeline
        p.run();
    }

}