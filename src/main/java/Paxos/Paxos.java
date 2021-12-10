package Paxos;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import Paxos.messages.AcceptMessage;
import Paxos.messages.AcceptOkMessage;
import Paxos.messages.PrepareMessage;
import Paxos.messages.PrepareOkMessage;
import Paxos.timers.PaxosTimer;
import protocols.agreement.IncorrectAgreement;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class Paxos extends GenericProtocol {
	

	private static final Logger logger = LogManager.getLogger(IncorrectAgreement.class);

	    //Protocol information, to register in babel
	    public final static short PROTOCOL_ID = 100;
	    public final static String PROTOCOL_NAME = "EmptyAgreement";
	    
	    private final int paxosTimeutTime; //param: timeout for Paxos

	    private Host myself;
	    private int joinedInstance;
	    private List<Host> membership;
	    
	    private Map<Integer, PaxosInstance> paxosInstances;
	    private long paxosTimer;
	    
	   
	   
	    public Paxos(Properties props) throws IOException, HandlerRegistrationException {
	    	 super(PROTOCOL_NAME, PROTOCOL_ID);
	         joinedInstance = -1; //-1 means we have not yet joined the system
	         membership = null;
	         paxosInstances= new HashMap<>();
	         
	         this.paxosTimeutTime = Integer.parseInt(props.getProperty("paxos_Time", "5000")); //5 seconds
	         
	         /*--------------------- Register Timer Handlers ----------------------------- */
	         registerTimerHandler(PaxosTimer.TIMER_ID, this::uponPaxosTimer);

	         /*--------------------- Register Request Handlers ----------------------------- */
	         registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
	         registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
	         registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

	         /*--------------------- Register Notification Handlers ----------------------------- */
	         subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
	         subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
	    }
	    
	    @Override
	    public void init(Properties props) {
	        //Nothing to do here, we just wait for events from the application or agreement
	    }
	    
	    
	    //Upon receiving the channelId from the membership, register our own callbacks and serializers
	    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
	        int cId = notification.getChannelId();
	        myself = notification.getMyself();
	        logger.info("Channel {} created, I am {}", cId, myself);
	        // Allows this protocol to receive events from this channel.
	        registerSharedChannel(cId);
	        
	        /*---------------------- Register Message Serializers ---------------------- */
	        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
	        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
	        registerMessageSerializer(cId, PrepareOkMessage.MSG_ID, PrepareOkMessage.serializer);
	        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
	        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);
	        /*---------------------- Register Message Handlers -------------------------- */
	        try {
	              registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
	              registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage);
	              registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage);
	              registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage);
	              registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage);
	        } catch (HandlerRegistrationException e) {
	            throw new AssertionError("Error registering message handler.", e);
	        }
	    }
	    
	    
	    
	    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
	        if(joinedInstance >= 0 ){
	            //Obviously your agreement protocols will not decide things as soon as you receive the first message
	            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
	        } else {
	            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
	            //agreement instances, maybe we should do something with them...?
	        }
	    }
	    
	    
	    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
	        //We joined the system and can now start doing things
	        joinedInstance = notification.getJoinInstance();
	        membership = new LinkedList<>(notification.getMembership());
	        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
	    }

	    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
	        logger.debug("Received " + request);
	        byte[] op = request.getOperation();
	        UUID opId= request.getOpId();
	        PaxosInstance p = new PaxosInstance(myself, membership);
	        paxosInstances.put(request.getInstance(), p);
	        p.setProposer_op(op,opId);
	        
	        PrepareMessage prepMsg;
	        for(Host member: membership) {
	        	prepMsg= new PrepareMessage(member,p.getProposer_seq(),request.getInstance());
	        	sendMessage(prepMsg, member);
	        }
	        p.setPrepate_ok_set(new TreeMap<Integer,PaxosOperation>());
	        
	        paxosTimer= setupTimer(new PaxosTimer(request.getInstance()), paxosTimeutTime);
	        
	        
	        //n deve ser preciso a parte de baixo em vez do for de cima
	        BroadcastMessage msg = new BroadcastMessage(request.getInstance(), request.getOpId(), request.getOperation());
	        logger.debug("Sending to: " + membership);
	        membership.forEach(h -> sendMessage(msg, h));
	    }
	    
	    private void uponPrepareMessage(PrepareMessage prepare,Host from, short sourceProto, int channelId) {
	    	PaxosInstance p= paxosInstances.get(prepare.getInstance());
	    	int sn=prepare.getProposer_Seq();
	    	if(sn > p.getHighest_prepare()) {
	    		p.setHighest_prepare(sn);
	    		PrepareOkMessage prepOkMsg=new PrepareOkMessage(from,sn,p.getHighest_accept(),p.getHighest_Op(),prepare.getInstance()); 
	    		sendMessage(prepOkMsg, from);
	    	}
	    	
	    }
	    
	    private void uponPrepareOkMessage(PrepareOkMessage prepareOk,Host from, short sourceProto, int channelId) {
	    	int sn= prepareOk.getProposer_Seq();
	    	int na= prepareOk.gethighAccept();
	    	PaxosOperation va= prepareOk.getHighOp();
	    	PaxosInstance p= paxosInstances.get(prepareOk.getInstance());
	    	if(p.getProposer_seq()==sn) {
	    		p.add_To_Prepate_ok_set(na, va);
	    		if(p.getSize_Prepate_ok_set() >= (membership.size()/2)+1) {
	    			Entry<Integer, PaxosOperation> highestEntry= p.getHighest_Of_Prepate_ok_set();
	    			if(highestEntry!=null && highestEntry.getValue()!=null) {
	    				PaxosOperation op= highestEntry.getValue();
	    				p.setProposer_op(op.getOp(),op.getOp_Id());
	    			}
	    			AcceptMessage acceptMsg;
	    			for(Host member: membership) {
	    				acceptMsg= new AcceptMessage(member,p.getProposer_seq(),p.getProposer_op(),prepareOk.getInstance());
	    	        	sendMessage(acceptMsg, member);
	    	        }
	    			p.setPrepate_ok_set(new TreeMap<Integer, PaxosOperation>() );
	    			this.cancelTimer(paxosTimer);
	    			paxosTimer= setupTimer(new PaxosTimer(prepareOk.getInstance()), paxosTimeutTime);
	    		}
	    	} 	
	    }
	    
	    private void uponAcceptMessage(AcceptMessage accept,Host from, short sourceProto, int channelId) {
	    	int sn= accept.getSeq();
	    	PaxosOperation op = accept.getOp();
	    	PaxosInstance p= paxosInstances.get(accept.getInstance());
	    	
	    	if(sn > p.getHighest_prepare()) {
	    		p.setHighest_prepare(sn);
	    		p.setHighest_accept(sn);
	    		p.setHighest_Op(op);
	    		AcceptOkMessage acceptOkMsg;
    			for(Host member: membership) {
    				acceptOkMsg=new AcceptOkMessage(member,sn,p.getHighest_Op(),accept.getInstance()); ;
    				sendMessage(acceptOkMsg, member);
    			}
	    	}
	    }
	    
	    
	    
	    private void uponAcceptOkMessage(AcceptOkMessage acceptOk,Host from, short sourceProto, int channelId) {
	    	int sn= acceptOk.getSeq();
	    	PaxosOperation op= acceptOk.getHighOp();
	    	PaxosInstance p= paxosInstances.get(acceptOk.getInstance());
	    	
	    	
	    	for(Entry<Integer, PaxosOperation> entry: p.getAccept_ok_set().entrySet()) {
	    		if(entry.getKey()==sn && entry.getValue().getOp()==op.getOp()) {
	    			p.add_To_Accept_ok_set(sn, op);
	    		}else if(entry.getKey()<sn) {
	    			TreeMap<Integer, PaxosOperation> newSet = new TreeMap<Integer, PaxosOperation>();
	    			newSet.put(sn, op);
	    			p.setPrepate_ok_set(newSet);
	    		}
	    	}
	    	
	    	if( p.getDecided()==null && p.getSize_Accept_ok_set() >= (membership.size()/2)+1 ) {
	    		p.setDecided(op);
	    		triggerNotification(new DecidedNotification(acceptOk.getInstance(), op.getOp_Id(), op.getOp()));
	    		if(sn==p.getProposer_seq()) {
	    			this.cancelTimer(paxosTimer);
	    		}
	    	}
	    }
	    
	    private void uponPaxosTimer(PaxosTimer pTimer, long timerId) {
	    	int instN=pTimer.getInstance();
	        logger.info("Paxos Timeout with instance number "+instN+" .");
	        PaxosInstance p = paxosInstances.get(instN);
	        if(p.getDecided()==null) {
	        	PrepareMessage prepMsg;
	 	        for(Host member: membership) {
	 	        	prepMsg= new PrepareMessage(member,p.getProposer_seq(),instN);
	 	        	sendMessage(prepMsg, member);
	 	        }
	 	        p.setPrepate_ok_set(new TreeMap<Integer,PaxosOperation>());
	 	        paxosTimer= setupTimer(new PaxosTimer(instN), paxosTimeutTime);
	        }
	    }
	    
	    
	    
	    
	    
	    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
	        logger.debug("Received " + request);
	        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
	        //You should probably take it into account while doing whatever you do here.
	        membership.add(request.getReplica());
	    }
	    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
	        logger.debug("Received " + request);
	        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
	        //You should probably take it into account while doing whatever you do here.
	        membership.remove(request.getReplica());
	    }

	    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
	        //If a message fails to be sent, for whatever reason, log the message and the reason
	        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	    }

}
