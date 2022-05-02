package edu.nwmissouri.bigdata.java.grp03_sowmya;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.beam.sdk.values.KV;
public class RankedPage implements Serializable, Comparator<KV<Double,String>> {
    String name = "unknown.md";
    Double rank =1.000;
    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();

    public RankedPage() {
    
    }
   
    public RankedPage(String nameIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }

    public RankedPage(String nameIn,Double rankIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.rank= rankIn;
        this.voters = votersIn;
    }
    public String getName(){
        return this.name;
    }
    public void setName(String nameIn){
        this.name = nameIn;
    }
    public double getRank(){
        return rank;
}

    public  ArrayList<VotingPage> getVoters(){
        return this.voters;
    }
   
    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }

@Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
    
    }
    
    @Override
    public int compare(KV<Double, String> r1, KV<Double, String> r2) {
        double rank1 = r1.getKey();
        double rank2 = r2.getKey();
        if (rank1 > rank2) {
            return 1;
        } else if(rank1 < rank2) {
            return -1;
        }else{
            return 0;
        }
    }}