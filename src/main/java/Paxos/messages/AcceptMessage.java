package Paxos.messages;

import io.netty.buffer.ByteBuf;
import Paxos.PaxosOperation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

 
    private final Host dest;
    private final int seq;
    private final int instance;
    private final PaxosOperation op;
    private final List<Host> membership;

    public AcceptMessage(Host dest, int seq, PaxosOperation op, int instance, List<Host> membership) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.seq =seq;
    	this.op=op;
    	this.instance = instance;
    	this.membership=membership;
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
    
    public List<Host> getMembership() {
        return membership;
    }

    
   /* @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }*/

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) throws IOException {
        	Host.serializer.serialize(msg.dest, out);
            out.writeInt(msg.instance);
            out.writeInt(msg.seq);
            out.writeLong(msg.op.getOp_Id().getMostSignificantBits());
            out.writeLong(msg.op.getOp_Id().getLeastSignificantBits());
            out.writeInt(msg.op.getOp().length);
            out.writeBytes(msg.op.getOp());
            int size=msg.membership.size();
            out.writeInt(size);
            for(Host h: msg.membership) {
            	Host.serializer.serialize(h, out);
            }
            
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) throws IOException {
        	Host dest = Host.serializer.deserialize(in);
            int instance = in.readInt();
            int seq = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int size = in.readInt();
            List<Host> members=new ArrayList<Host>();
            for(int i=0;i<size;i++) {
            	Host h = Host.serializer.deserialize(in);
            	members.add(h);
            }
            PaxosOperation po = new PaxosOperation(op, opId);
            return new AcceptMessage(dest, seq, po, instance,members);
        }
    };
   
    

}
