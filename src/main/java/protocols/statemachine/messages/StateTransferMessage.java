package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import protocols.app.messages.RequestMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.nio.charset.StandardCharsets;

public class StateTransferMessage extends ProtoMessage {
    public final static short MSG_ID = 201;

    public StateTransferMessage() {
        super(MSG_ID);
    }

    public static ISerializer<StateTransferMessage> serializer = new ISerializer<StateTransferMessage>() {
        @Override
        public void serialize(StateTransferMessage requestMessage, ByteBuf out) {

        }

        @Override
        public StateTransferMessage deserialize(ByteBuf in) {
          return new StateTransferMessage();
        }
    };
}
