package Paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

public class PaxosInstance {
	
	private PaxosOperation proposer_op;
    private int proposer_seq;
    private int highest_prepare;
    private int highest_accept;
    private PaxosOperation highest_Op;
    private TreeMap<Integer, ArrayList<PaxosOperation>> prepare_ok_set;
    private TreeMap<Integer,ArrayList<PaxosOperation>> accept_ok_set;
    private List<Host> membership;
    private PaxosOperation decided;
    private long timeOutId;
	private int idx;
	private static final Logger logger = LogManager.getLogger(PaxosInstance.class);
    
    
    public PaxosInstance(Host localProcessId,List<Host> membership,int idx) {
		this.idx=idx;
    	proposer_seq=idx;
    	this.membership=membership;
    	proposer_op=null;
    	highest_prepare=0;
    	highest_accept=0;
    	highest_Op=null;
    	
    	prepare_ok_set=new TreeMap<Integer, ArrayList<PaxosOperation>>();
    	accept_ok_set = new TreeMap<Integer, ArrayList<PaxosOperation>>();
    	decided=null;
    	timeOutId=-1;
    }

	public int getIdx(){
		return idx;
	}
    
    public void setTimer(long timeoutId) {
    	this.timeOutId=timeoutId;
    }

    public long getTimer() {
    	return timeOutId;
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


	public TreeMap<Integer, ArrayList<PaxosOperation>> getPrepare_ok_set() {

		return prepare_ok_set;
	}


	public void setPrepare_ok_set(TreeMap<Integer,ArrayList<PaxosOperation>> prepate_ok_set) {
		this.prepare_ok_set = prepate_ok_set;
	}
	
	public void add_To_Prepare_ok_set(int seqN, PaxosOperation operation ){
		ArrayList<PaxosOperation> paxList=prepare_ok_set.get(seqN);
		if(paxList==null) {
			paxList= new ArrayList<>();
		}
		paxList.add(operation);
		prepare_ok_set.put(seqN, paxList);
	}
	
	public int getSize_Prepare_ok_set(int sn) {
		if(prepare_ok_set.get(sn)==null) {
			return 0;
		}
		return prepare_ok_set.get(sn).size();
	}
	
	public Entry<Integer, ArrayList<PaxosOperation>> getHighest_Of_Prepare_ok_set() {
		return prepare_ok_set.lastEntry();
	}



	public TreeMap<Integer,  ArrayList<PaxosOperation>> getAccept_ok_set() {
		return accept_ok_set;
	}
	
	public int getSize_Accept_ok_set() {
		if(accept_ok_set.firstEntry()==null) {
			return 0;
		}
		return accept_ok_set.firstEntry().getValue().size();
	}
	
	public void add_To_Accept_ok_set(int seqN, PaxosOperation operation ){
		ArrayList<PaxosOperation> paxList=accept_ok_set.get(seqN);
		if(paxList==null) {
			paxList=new ArrayList<PaxosOperation>();
		}
		paxList.add(operation);
		accept_ok_set.put(seqN, paxList);
	}


	public void setAccept_ok_set(TreeMap<Integer, ArrayList<PaxosOperation>> accept_ok_set) {
		this.accept_ok_set = accept_ok_set;
	}


	public List<Host> getMembership() {
		return membership;
	}


	public void setMembership(List<Host> membership) {
		this.membership = membership;
	}


	public PaxosOperation getDecided() {
		return decided;
	}


	public void setDecided(PaxosOperation op) {
		this.decided = op;
	}
}
