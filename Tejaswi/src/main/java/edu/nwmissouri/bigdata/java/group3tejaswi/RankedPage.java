package edu.nwmissouri.bigdata.java.group3tejaswi;

import java.util.ArrayList;

public class RankedPage {
    String  key;
    ArrayList<VotingPage> voters;

    public RankedPage(String key, ArrayList<VotingPage> voters) 
    {
        this.key = key;
        this.voters = voters;
    }
     public String getKey(){
        return key;
    }
    public void setKey(String key){
        this.key = key;
    }
    public   ArrayList<VotingPage> getVoters(){
        return voters;
    }
    public  void setVoters( ArrayList<VotingPage> voters){
        this.voters = voters;
    }

}
