package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class StateTransferReplyMessage extends ProtoMessage {
    public final static short MSG_ID = 202;

    private int instance;
    private byte[] state;

    public StateTransferReplyMessage(int instance, byte[] state) {
        super(MSG_ID);
        this.instance = instance;
        this.state = state;
    }

    public int getInstance() {
        return instance;
    }

    public byte[] getState() {
        return state;
    }

    public static ISerializer<StateTransferReplyMessage> serializer = new ISerializer<StateTransferReplyMessage>() {
        @Override
        public void serialize(StateTransferReplyMessage requestMessage, ByteBuf out) {
            out.writeInt(requestMessage.instance);
            out.writeBytes(requestMessage.state);
        }

        @Override
        public StateTransferReplyMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            byte[] state = new byte[in.readableBytes()];

            return new StateTransferReplyMessage(instance, state);
        }
    };
}

