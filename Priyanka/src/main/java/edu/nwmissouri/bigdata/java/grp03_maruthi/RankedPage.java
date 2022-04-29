package edu.nwmissouri.bigdata.java.grp03_maruthi;

import java.util.ArrayList;

import java.io.Serializable;
public class RankedPage implements Serializable{
    String voter;
    ArrayList<VotingPage> voterList = new ArrayList<>();
    
    public RankedPage(String voter, ArrayList<VotingPage> voters){
        this.voter = voter;
        this.voterList = voters;
    }
    
    public String getVoter() {
        return voter;
    }

    public void setVoter(String voter) {
        this.voter = voter;
    }

    public ArrayList<VotingPage> getVoterList() {
        return voterList;
    }

    public void setVoterList(ArrayList<VotingPage> voterList) {
        this.voterList = voterList;
    }

    @Override
    public String toString(){
        return voter + voterList;
    }
}
