package Paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;

import Paxos.PaxosOperation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

 
    private final Host dest;
    private final int seq;
    private final int instance;
    private final PaxosOperation op;

    public AcceptMessage(Host dest, int seq, PaxosOperation op, int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.seq =seq;
    	this.op=op;
    	this.instance = instance;
	}

	public int getSeq() {
        return seq;
    }
	
	public PaxosOperation getOp() {
        return op;
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
