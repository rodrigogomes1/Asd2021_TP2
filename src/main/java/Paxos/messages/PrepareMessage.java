package Paxos.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;


public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 102;

 
    private final Host dest;
    private final int proposer_seq;
    private final int instance;

    public PrepareMessage(Host dest, int proposer_seq,int instance) {
    	super(MSG_ID);
    	this.dest=dest;
    	this.proposer_seq =proposer_seq;
    	this.instance = instance;
	}

	public int getProposer_Seq() {
        return proposer_seq;
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

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) throws IOException {
        	Host.serializer.serialize(msg.dest, out);
        	out.writeInt(msg.proposer_seq);
            out.writeInt(msg.instance);   
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) throws IOException {
        	Host dest = Host.serializer.deserialize(in);
            int proposer_seq = in.readInt();
            int instance = in.readInt();
            return new PrepareMessage(dest, proposer_seq, instance);
        }
    };
   
    

}
