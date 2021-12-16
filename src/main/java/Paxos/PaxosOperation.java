package Paxos;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class PaxosOperation {
	private byte[] op;
	private UUID op_Id;
	
	public PaxosOperation(byte[] op,UUID op_Id) {
		this.op=op;
		this.op_Id=op_Id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PaxosOperation that = (PaxosOperation) o;
		return Arrays.equals(op, that.op) && Objects.equals(op_Id, that.op_Id);
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
