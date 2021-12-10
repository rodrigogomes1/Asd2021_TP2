package Paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class AcceptOkMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

 
    private final Host dest;
    private final int seq;
    private final byte[] highOp;
    private final int instance;

    public AcceptOkMessage(Host dest, int seq, byte[] highOp, int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.seq =seq;
		this.highOp = highOp;
		this.instance = instance;
	}

	public int getSeq() {
        return seq;
    }
	
	public byte[] getHighOp() {
        return highOp;
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

    public static ISerializer<AcceptOkMessage> serializer = new ISerializer<AcceptOkMessage>() {
        @Override
        public void serialize(AcceptOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new AcceptOkMessage(instance, opId, op);
        }
    };
   
    

}
