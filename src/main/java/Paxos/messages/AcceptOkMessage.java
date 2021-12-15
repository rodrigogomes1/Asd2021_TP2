package Paxos.messages;

import io.netty.buffer.ByteBuf;
import Paxos.PaxosOperation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;


public class AcceptOkMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

 
    private final Host dest;
    private final int seq;
    private final PaxosOperation highOp;
    private final int instance;

    public AcceptOkMessage(Host dest, int seq, PaxosOperation highOp, int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.seq =seq;
		this.highOp = highOp;
		this.instance = instance;
	}

	public int getSeq() {
        return seq;
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

    
   /* @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }*/

    public static ISerializer<AcceptOkMessage> serializer = new ISerializer<AcceptOkMessage>() {
        @Override
        public void serialize(AcceptOkMessage msg, ByteBuf out) throws IOException {
        	Host.serializer.serialize(msg.dest, out);
            out.writeInt(msg.instance);
            out.writeInt(msg.seq);
            if(msg.highOp == null)
                out.writeInt(1);
            else {
                out.writeInt(0);
                out.writeLong(msg.highOp.getOp_Id().getMostSignificantBits());
                out.writeLong(msg.highOp.getOp_Id().getLeastSignificantBits());
                out.writeInt(msg.highOp.getOp().length);
                out.writeBytes(msg.highOp.getOp());
            }
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) throws IOException {
        	Host dest = Host.serializer.deserialize(in);
            int instance = in.readInt();
            int seq = in.readInt();
            int isNull = in.readInt();

            PaxosOperation po;
            if(isNull == 1)
                po = null;
            else{
                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                UUID opId = new UUID(highBytes, lowBytes);
                byte[] op = new byte[in.readInt()];
                in.readBytes(op);
                po = new PaxosOperation(op, opId);
            }
            return new AcceptOkMessage(dest, seq, po, instance);
        }
    };
   
    

}
