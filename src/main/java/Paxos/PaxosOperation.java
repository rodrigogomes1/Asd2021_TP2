package Paxos;

import java.util.UUID;

public class PaxosOperation {
	private byte[] op;
	private UUID op_Id;
	
	public PaxosOperation(byte[] op,UUID op_Id) {
		this.op=op;
		this.op_Id=op_Id;
	}

	public byte[] getOp() {
		return op;
	}

	public void setOp(byte[] op) {
		this.op = op;
	}

	public UUID getOp_Id() {
		return op_Id;
	}

	public void setOp_Id(UUID op_Id) {
		this.op_Id = op_Id;
	}
	
	

}
