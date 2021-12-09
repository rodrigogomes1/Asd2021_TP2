package Paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 102;

 
    private final Host dest;
    private final int proposer_seq;
    private final int instance;
    private final byte[] proposer_op;

    public AcceptMessage(Host dest, int proposer_seq, byte[] proposer_op, int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.proposer_seq =proposer_seq;
    	this.proposer_op=proposer_op;
    	this.instance = instance;
	}

	public int getProposer_Seq() {
        return proposer_seq;
    }
	
	public byte[] getProposer_Op() {
        return proposer_op;
    }

	
	 public int getInstance() {
	        return instance;
	    }


    public Host getDest() {
        return dest;
    }

    
    @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new AcceptMessage(instance, opId, op);
        }
    };
   
    

}
