package Paxos;

import java.util.List;
import java.util.TreeMap;

import pt.unl.fct.di.novasys.network.data.Host;

public class PaxosInstance {
	
	private byte[] proposer_op;
    private int proposer_seq;
    private int highest_prepare;
    private int highest_accept;
    private byte[] highest_value;
    private TreeMap<Integer, byte[]> prepate_ok_set;
    private TreeMap<Integer, byte[]> accept_ok_set;
    private List<Host> membeship;
    private  byte[] decided;
    
    
    public PaxosInstance(Host localProcessId,List<Host> membership) {
    	proposer_seq=localProcessId.hashCode();//pode dar mal
    	this.membeship=membership;
    	proposer_op=null;
    	highest_prepare=0;
    	highest_accept=0;
    	highest_value=null;
    	prepate_ok_set=new TreeMap<Integer, byte[]>();
    	accept_ok_set = new TreeMap<Integer, byte[]>();
    	decided=null;
    }


	public byte[] getProposer_op() {
		return proposer_op;
	}


	public void setProposer_op(byte[] proposer_op) {
		this.proposer_op = proposer_op;
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


	public byte[] getHighest_value() {
		return highest_value;
	}


	public void setHighest_value(byte[] highest_value) {
		this.highest_value = highest_value;
	}


	public TreeMap<Integer, byte[]> getPrepate_ok_set() {
		return prepate_ok_set;
	}


	public void setPrepate_ok_set(TreeMap<Integer, byte[]> prepate_ok_set) {
		this.prepate_ok_set = prepate_ok_set;
	}


	public TreeMap<Integer, byte[]> getAccept_ok_set() {
		return accept_ok_set;
	}


	public void setAccept_ok_set(TreeMap<Integer, byte[]> accept_ok_set) {
		this.accept_ok_set = accept_ok_set;
	}


	public List<Host> getMembeship() {
		return membeship;
	}


	public void setMembeship(List<Host> membeship) {
		this.membeship = membeship;
	}


	public byte[] getDecided() {
		return decided;
	}


	public void setDecided(byte[] decided) {
		this.decided = decided;
	}
}
