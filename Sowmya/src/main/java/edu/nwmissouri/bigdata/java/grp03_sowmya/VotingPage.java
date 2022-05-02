package edu.nwmissouri.bigdata.java.grp03_sowmya;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public  class VotingPage implements Serializable{
    String name = "unknown.md";
    Double rank = 1.0;
    Integer votes = 0;

    public VotingPage(String nameIn, Integer votesIn) {
        this.name = nameIn;
        this.votes = votesIn;
    }
    
    public VotingPage(String nameIn,Double rankIn, Integer votesIn) {
        this.name = nameIn;
        this.rank = rankIn;
        this.votes = votesIn;
    }
   
    public String getName(){
        return this.name;
    }
    public Double getRank(){
       return  this.rank;
    }
    public  Integer getVotes(){
        return this.votes;
    }
@Override
    public String toString(){
        return String.format("%s,%.5f,%d", this.name,this.rank,this.votes);
    }

}  