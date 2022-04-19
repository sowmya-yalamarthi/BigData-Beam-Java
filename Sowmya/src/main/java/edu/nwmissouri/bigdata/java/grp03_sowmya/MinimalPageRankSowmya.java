/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.bigdata.java.grp03_sowmya;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.Arrays;

// import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;


public class MinimalPageRankSowmya {

  // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }


  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
   
   PCollection<KV<String,String>> pCollKVPairs1 = SowmyaMapper01(p,"go.md",dataFolder);
   PCollection<KV<String,String>> pCollKVPairs2 = SowmyaMapper01(p,"python.md",dataFolder);
   PCollection<KV<String,String>> pCollKVPairs3 = SowmyaMapper01(p,"java.md",dataFolder);
   PCollection<KV<String,String>> pCollKVPairs4 = SowmyaMapper01(p,"README.md",dataFolder);

   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(pCollKVPairs1).and(pCollKVPairs2).and(pCollKVPairs3).and(pCollKVPairs4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());
    
    PCollection<String> pLinksString = mergedList.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    pLinksString.apply(TextIO.write().to("SowmyaPageRank"));  
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> SowmyaMapper01(Pipeline p, String filename, String dataFolder){
   
    String newdataPath = dataFolder + "/" + filename;
     PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
     PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }

}
    // p.run().waitUntilFinish();
            
    // PCollection<String> pcolInput =
    //                          p.apply(TextIO.read().from(datapath));
    //  .apply(Filter.by((String line) -> !line.isEmpty()))
    //  .apply(Filter.by((String line) -> !line.equals(" ")))
    // PCollection<String> pcollinkLines = 
    //                          pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));

    // PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.
    //               into((TypeDescriptors.strings()))
    //              .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));



        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        // .apply(
        //     FlatMapElements.into(TypeDescriptors.strings())
        //         .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        // // We use a Filter transform to avoid empty word
        // .apply(Filter.by((String word) -> !word.isEmpty()))
        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        // .apply(Count.perElement())
        // Apply a MapElements transform that formats our PCollection of word counts into a
        // printable string, suitable for writing to an output file.
        // .apply(
        //     MapElements.into(TypeDescriptors.strings())
        //         .via(
        //             (KV<String, Long> wordCount) ->
        //                 wordCount.getKey() + ": " + wordCount.getValue()))
        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        
    //     PDone pcol = pcolLinks.apply(TextIO.write().to("sowmyaRank"));

    // p.run().waitUntilFinish();
 