package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class RemoveReplicaRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final int instance;
    private final Host replica;

    public RemoveReplicaRequest(int instance, Host replica) {
        super(REQUEST_ID);
        this.instance = instance;
        this.replica = replica;
    }

    public int getInstance() {
        return instance;
    }

    public Host getReplica() {
    	return replica;
    }
   

    @Override
    public String toString() {
        return "ProposeRequest{" +
                "instance=" + instance +
                ", replica=" + replica +
                '}';
    }
}
