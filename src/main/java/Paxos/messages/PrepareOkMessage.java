package Paxos.messages;

import io.netty.buffer.ByteBuf;
import Paxos.PaxosOperation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;


public class PrepareOkMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

 
    private final Host dest;
    private final int proposer_seq;
    private final int highAccept;
    private final PaxosOperation highOp;
    private final int instance;

    public PrepareOkMessage(Host dest, int proposer_seq, int highAccept, PaxosOperation highOp, int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.proposer_seq =proposer_seq;
		this.highAccept = highAccept;
		this.highOp = highOp;
		this.instance = instance;
	}

	public int getProposer_Seq() {
        return proposer_seq;
    }
	
	public int gethighAccept() {
        return highAccept;
    }
	
	public PaxosOperation getHighOp() {
        return highOp;
    }

	
 	public int getInstance() {
	        return instance;
    }


    public Host getDest() {
        return dest;
    }

    
  /*  @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }*/

    public static ISerializer<PrepareOkMessage> serializer = new ISerializer<PrepareOkMessage>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) throws IOException {
        	Host.serializer.serialize(msg.dest, out);
            out.writeInt(msg.instance);
            out.writeInt(msg.proposer_seq);
            out.writeInt(msg.highAccept);
            out.writeLong(msg.highOp.getOp_Id().getMostSignificantBits());
            out.writeLong(msg.highOp.getOp_Id().getLeastSignificantBits());
            out.writeInt(msg.highOp.getOp().length);
            out.writeBytes(msg.highOp.getOp());
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) throws IOException {
        	Host dest = Host.serializer.deserialize(in);
            int instance = in.readInt();
            int proposer_seq = in.readInt();
            int highAccept = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            PaxosOperation po = new PaxosOperation(op, opId);
            return new PrepareOkMessage(dest, proposer_seq, highAccept, po, instance);
        }
    };
   
    

}
