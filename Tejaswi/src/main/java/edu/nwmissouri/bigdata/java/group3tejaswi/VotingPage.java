package edu.nwmissouri.bigdata.java.group3tejaswi;

public  class VotingPage extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPage>> implements Serializable{
    String voterName;
    int contributorVotes;
    public VotingPage(String voterName,Integer contributorVotes2){
        this.voterName = voterName;
        this.contributorVotes = contributorVotes2;        
    }
    public String getVoterName() {
        return voterName;
    }
    public void setVoterName(String voterName) {
        this.voterName = voterName;
    }
    public int getContributorVotes() {
        return contributorVotes;
    }
    public void setContributorVotes(int contributorVotes) {
        this.contributorVotes = contributorVotes;
    }
    @Override
    public String toString() {
        return "contributorVotes=" + contributorVotes + ", voterName=" + voterName;
    }
    
}
