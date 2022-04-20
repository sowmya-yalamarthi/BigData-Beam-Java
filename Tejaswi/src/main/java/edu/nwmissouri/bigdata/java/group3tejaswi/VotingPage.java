package edu.nwmissouri.bigdata.java.group3tejaswi;

public class VotingPage {
    public String voterName;
    public Integer numberofVotes;
    public VotingPage(String voterName, Integer numberofVotes) {
        this.voterName = voterName;
        this.numberofVotes = numberofVotes;
    }
    public String getVoterName(){
        return voterName;
    }
    public  Integer getNumberOfVotes(){
        return numberofVotes;
    }
    public void setVoterName(String voterName){
        this.voterName = voterName;
    }
    public void setNumberOfVotes(Integer numberofVotes ){
        this.numberofVotes = numberofVotes;
    }
}
