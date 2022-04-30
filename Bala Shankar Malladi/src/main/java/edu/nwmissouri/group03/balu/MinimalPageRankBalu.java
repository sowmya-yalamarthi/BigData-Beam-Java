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
package edu.nwmissouri.group03.balu;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankBalu {

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

  // JOB2 MAPPER
 static class Job2Mapper extends DoFn<KV<String,RankedPage >, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, RankedPage> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<VotingPage> voters =  element.getValue().getVoters();
      if ( voters instanceof Collection){
           
        votes =((Collection<VotingPage>)voters).size();
    }
    for (VotingPage vp: voters){
      String pageName = vp.getName();
      Double pageRank = vp.getRank();
      String contributorPageName= element.getKey();
      Double contributorPageRank= element.getValue().getRank();
      VotingPage contributor = new VotingPage(contributorPageName,contributorPageRank,votes);
      ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
      arr.add(contributor);
      receiver.output(KV.of(vp.getName(), new RankedPage(pageName,pageRank,arr)));
    }
  }
}
  
   // JOB2 UPDATER
static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, Iterable<RankedPage>> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
    
        String thisPage = element.getKey();
        Iterable<RankedPage> rankedPages = element.getValue();
        Double dampfactor = 0.85;
        Double updateRank = (1.0 -dampfactor);
        ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();

      for (RankedPage pg:rankedPages) {
      if (pg!=null) {
        for(VotingPage vp :pg.getVoters()){
          newVoters.add(vp);
          updateRank +=(dampfactor)*vp.getRank()/(double)vp.getVotes();
        }
      }
    }
    receiver.output(KV.of(thisPage, new RankedPage(thisPage,updateRank,newVoters)));
  }
}

// JOB3 FINALIZER
static class Job3Finalizer extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
  @ProcessElement
  public void processElement(@Element KV<String, RankedPage> element,
      OutputReceiver<KV<Double, String>> receiver) {
    receiver.output(KV.of(element.getValue().getRank(), element.getKey()));
  }
}
public static  void deleteFiles(){
  final File file = new File("./");
  for (File f : file.listFiles()){
   if(f.getName().startsWith("baluOutput")){
  f.delete();
  }
  }
 }
   // Map to KV pairs
  private static PCollection<KV<String, String>> baluMapper1(Pipeline p, String dataFolder, String dataFile) {

    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pColInputLines = p.apply(TextIO.read().from(dataPath));
    PCollection<String> pColLinkLines = pColInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pColLinkedPages = pColLinkLines.apply(MapElements.into(TypeDescriptors.strings())
        .via((String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> pColKvPairs = pColLinkedPages.apply(MapElements
        .into(TypeDescriptors.kvs(
            TypeDescriptors.strings(), TypeDescriptors.strings()

        ))
        .via(outlink -> KV.of(dataFile, outlink)));
    return pColKvPairs;

  }
 /**
   * Run one iteration of the Job 2 Map-Reduce process
   * Notice how the Input Type to Job 2.
   * Matches the Output Type from Job 2.
   * How important is that for an iterative process?
   * 
   * @param kvReducedPairs - takes a PCollection<KV<String, RankedPage>> with
   *                       initial ranks.
   * @return - returns a PCollection<KV<String, RankedPage>> with updated ranks.
   */
 private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {
     PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));
   // apply(ParDo.of(new Job2Mapper()));

    // KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
    // KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());

    // KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
    // KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
    // 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));
    // KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
    // python.md, 1.00000,1]}
    // KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}
    return updatedOutput;
  }

 // MAIN METHOD

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
    // Processing all four input files
    PCollection<KV<String, String>> pCollection1 = baluMapper1(p, dataFolder, "go.md");
    PCollection<KV<String, String>> pCollection2 = baluMapper1(p, dataFolder, "java.md");
    PCollection<KV<String, String>> pCollection3 = baluMapper1(p, dataFolder, "python.md");
    PCollection<KV<String, String>> pCollection4 = baluMapper1(p, dataFolder, "README.md");
    // Make a List of PCs
    PCollectionList<KV<String, String>> pCollectionsList = PCollectionList.of(pCollection1).and(pCollection2)
        .and(pCollection3).and(pCollection4);
    // Flatten into a Merged PCollection
    PCollection<KV<String, String>> mergedPcollection = pCollectionsList
        .apply(Flatten.<KV<String, String>>pCollections());

    // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> kvReducedPairs = mergedPcollection.apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = kvReducedPairs.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null; 
    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      job2out= runJob2Iteration(job2in);
      job2in =job2out;
    }
    
    PCollection<KV<Double, String>> job3out = job2out.apply(ParDo.of(new Job3Finalizer()));
    PCollection<KV<Double, String>> maxFinalPageRank = job3out.apply(Combine.globally(Max.of(new RankedPage())));
    PCollection<String> mergeString = maxFinalPageRank.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((kvInput) -> kvInput.toString()));
    mergeString.apply(TextIO.write().to("baluOutput"));

    p.run().waitUntilFinish();
  
  }
}