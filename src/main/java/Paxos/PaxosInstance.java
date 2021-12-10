package Paxos;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import pt.unl.fct.di.novasys.network.data.Host;

public class PaxosInstance {
	
	private PaxosOperation proposer_op;
    private int proposer_seq;
    private int highest_prepare;
    private int highest_accept;
    private PaxosOperation highest_Op;
    private TreeMap<Integer, PaxosOperation> prepate_ok_set;
    private TreeMap<Integer,PaxosOperation> accept_ok_set;
    private List<Host> membership;
    private PaxosOperation decided;
    
    
    public PaxosInstance(Host localProcessId,List<Host> membership) {
    	proposer_seq=localProcessId.hashCode();//pode dar mal
    	this.membership=membership;
    	proposer_op=null;
    	highest_prepare=0;
    	highest_accept=0;
    	highest_Op=null;
    	
    	prepate_ok_set=new TreeMap<Integer, PaxosOperation>();
    	accept_ok_set = new TreeMap<Integer, PaxosOperation>();
    	decided=null;
    }


	public PaxosOperation getProposer_op() {
		return proposer_op;
	}


	public void setProposer_op(byte[] proposer_op,UUID opId) {
		PaxosOperation newOp= new PaxosOperation(proposer_op, opId);
		this.proposer_op = newOp;
	}


	public int getProposer_seq() {
		return proposer_seq;
	}


	public void setProposer_seq(int proposer_seq) {
		this.proposer_seq = proposer_seq;
	}


	public int getHighest_prepare() {
		return highest_prepare;
	}


	public void setHighest_prepare(int highest_prepare) {
		this.highest_prepare = highest_prepare;
	}


	public int getHighest_accept() {
		return highest_accept;
	}


	public void setHighest_accept(int highest_accept) {
		this.highest_accept = highest_accept;
	}


	public PaxosOperation getHighest_Op() {
		return highest_Op;
	}


	public void setHighest_Op(PaxosOperation op) {
		this.highest_Op = op;
	}


	public TreeMap<Integer, PaxosOperation> getPrepate_ok_set() {
		return prepate_ok_set;
	}


	public void setPrepate_ok_set(TreeMap<Integer,PaxosOperation> prepate_ok_set) {
		this.prepate_ok_set = prepate_ok_set;
	}
	
	public void add_To_Prepate_ok_set(int seqN, PaxosOperation operation ){
		prepate_ok_set.put(seqN, operation);
	}
	
	public int getSize_Prepate_ok_set() {
		return prepate_ok_set.keySet().size();
	}
	
	public Entry<Integer, PaxosOperation> getHighest_Of_Prepate_ok_set() {
		return prepate_ok_set.lastEntry();
	}



	public TreeMap<Integer, PaxosOperation> getAccept_ok_set() {
		return accept_ok_set;
	}
	
	public int getSize_Accept_ok_set() {
		return accept_ok_set.keySet().size();
	}
	
	public void add_To_Accept_ok_set(int seqN, PaxosOperation operation ){
		accept_ok_set.put(seqN, operation);
	}


	public void setAccept_ok_set(TreeMap<Integer, PaxosOperation> accept_ok_set) {
		this.accept_ok_set = accept_ok_set;
	}


	public List<Host> getMembeship() {
		return membership;
	}


	public void setMembeship(List<Host> membeship) {
		this.membership = membeship;
	}


	public PaxosOperation getDecided() {
		return decided;
	}


	public void setDecided(PaxosOperation op) {
		this.decided = op;
	}
}
