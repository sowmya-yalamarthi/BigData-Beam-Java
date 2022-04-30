package edu.nwmissouri.bigdata.java.grp03_sowmya;

import java.util.ArrayList;

import java.io.Serializable;
public class RankedPage implements Serializable{
  public String name;
    public ArrayList<VotingPage> voters;
    public double rank;


    public RankedPage(String voter,double rank, ArrayList<VotingPage> voters){
        this.name = voter;
        this.voters = voters;
        this.rank = rank;
    }  

    public RankedPage(String key, ArrayList<VotingPage> voters) {
        this.name = key;
        this.voters = voters;
        this.rank = 1.0;
    }

    public String getKey(){
        return name;
    }

    public  ArrayList<VotingPage> getVoters(){
        return voters;
    }

    public void setKey(String key){
        this.name = key;
    }

    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }

    @Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.voters.toString());
        //return "RankedPage [name=" + name + ", voterList=" + voters + "rank= "+this.rank+"]";
    }
    public double getRank() {
        return this.rank;
    }


 @Override
    public int compare(KV<Double, String> o1, KV<Double, String> o2) {
        double rank1 = o1.getKey();
        double rank2 = o2.getKey();
        if (rank1 > rank2) {
            return 1;
        } else if(rank1 < rank2) {
            return -1;
        }else{
            return 0;
        }
    }

}
