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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.yuriatgoogle.basicpipeline.StringToRowConverter;





public class basicpipeline {

    private static final Logger LOG = LoggerFactory.getLogger(basicpipeline.class);
    //Google Cloud settings
    public static String topicID = "test-topic"; //pubsub topic
    public static String projectId = "ymg-basic-pipeline"; //project ID
    public static String subscription = "projects/" + projectId + "/" + topicID + "/subscriptions/test-subscription"; //pubsub subscription
    public static String readTopic = "projects/" + projectId + "/topics/" + topicID;
    public static String datasetId = "dataset"; //BigQuery dataset
    public static String tableId = "testTable"; //BigQuery table
    public static String bucketId = "gs://ymg-dataflow-bucket"; //staging bucket for dataflow
    private static final String tableSchema = "{\"fields\":[{\"type\":\"STRING\",\"name\":\"message\",\"mode\":\"NULLABLE\"}]}"; //schema for the BQ table

        
    //create table schema from Json
    private static TableSchema createTableSchema(String schema) throws IOException {
        return JacksonFactory.getDefaultInstance().fromString(schema, TableSchema.class);
    }


    // Converts strings into BigQuery rows.
    static class StringToRowConverter extends DoFn<String, TableRow> {
        // * In this example, put the whole string into single BigQuery field.
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(new TableRow().set("message", c.element()));
        }
    
        static TableSchema getSchema() {
          return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                // Compose the list of TableFieldSchema from tableSchema.
                {
                  add(new TableFieldSchema().setName("message").setType("STRING")); //BQ field name
                }
          });
        }
      }


    public static void main(String[] args) {

    	// Setup Dataflow options
        StreamingOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamingOptions.class);
        options.setStreaming(true);


        Pipeline pipeline = Pipeline.create(options);

        //BQ table setup
        TableSchema bqTableSchema;
        try {
            bqTableSchema = createTableSchema(tableSchema);
        } catch (IOException e){
            e.printStackTrace();
            return;
        }
        
        String tableName = projectId + ":" + datasetId + "." + tableId;
        
        Pipeline p = Pipeline.create(options);

        // Read message from Pub/Sub
        p.apply("ReadFromPubSub", PubsubIO.readStrings()
            .fromTopic(readTopic))
        
        // Format tweets for BigQuery - convert string to table row
        .apply("Format for BigQuery", ParDo.of(new StringToRowConverter()))

        
        // Write tweets to BigQuery
        .apply("write to BQ", BigQueryIO.writeTableRows()
            .to(tableName)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
            .withSchema(bqTableSchema));

        //run pipeline
        PipelineResult result = p.run();
    }

}