package Paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class PrepareOkMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

 
    private final Host dest;
    private final int proposer_seq;
    private final int highAccept;
    private final byte[] highOp;
    private final int instance;

    public PrepareOkMessage(Host dest, int proposer_seq, int highAccept, byte[] highOp, int instance) {
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

    public static ISerializer<PrepareOkMessage> serializer = new ISerializer<PrepareOkMessage>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new PrepareOkMessage(instance, opId, op);
        }
    };
   
    

}
