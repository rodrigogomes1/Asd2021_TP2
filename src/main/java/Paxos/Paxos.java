package Paxos;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import Paxos.messages.AcceptMessage;
import Paxos.messages.AcceptOkMessage;
import Paxos.messages.PrepareMessage;
import Paxos.messages.PrepareOkMessage;
import Paxos.timers.PaxosTimer;
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


	private static final Logger logger = LogManager.getLogger(Paxos.class);

	//Protocol information, to register in babel
	public final static short PROTOCOL_ID = 100;
	public final static String PROTOCOL_NAME = "Paxos";

	private final int paxosTimeutTime; //param: timeout for Paxos

	private Host myself;
	private int joinedInstance;
	private List<Host> membership;

	private int maxTimeOuts=0;
	private int idx;

	private Map<Integer, PaxosInstance> paxosInstances;



	public Paxos(Properties props) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		joinedInstance = -1; //-1 means we have not yet joined the system
		membership = null;
		paxosInstances= new HashMap<>();


		this.paxosTimeutTime = Integer.parseInt(props.getProperty("paxos_Time", "1000")); //5 seconds

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
		//logger.info("Channel {} created, I am {}", cId, myself);
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
			registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage);
			registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage);
			registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage);
			registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage);
		} catch (HandlerRegistrationException e) {
			throw new AssertionError("Error registering message handler.", e);
		}
	}

	private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
		//We joined the system and can now start doing things
		joinedInstance = notification.getJoinInstance();
		membership = new LinkedList<>(notification.getMembership());
		idx=getIndexReplica();
		//logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
	}
	private int getIndexReplica(){
		int i=0;
		for(Host h:membership){
			if(myself.equals(h)){
				return i;
			}
			i++;
		}
		return i;
	}

	private void uponProposeRequest(ProposeRequest request, short sourceProto) {
		//logger.info("UponProposeRequest request in instance {} with membership size {}", request.getInstance(), membership.size());

		byte[] op = request.getOperation();
		UUID opId= request.getOpId();

		PaxosInstance p= paxosInstances.get(request.getInstance());
		if(p==null) {
			p = new PaxosInstance(myself, membership,idx);
			paxosInstances.put(request.getInstance(), p);
		}
		p.setProposer_op(op,opId);

		PrepareMessage prepMsg;
		for(Host member: p.getMembership()) {
			prepMsg= new PrepareMessage(member,p.getProposer_seq(),request.getInstance());
			sendMessage(prepMsg, member);
		}
		p.setPrepare_ok_set(new TreeMap<>());

		long paxosTimerId= setupTimer(new PaxosTimer(request.getInstance()), paxosTimeutTime);
		p.setTimer(paxosTimerId);

		paxosInstances.put(request.getInstance(), p);
	}

	private void uponPrepareMessage(PrepareMessage prepare,Host from, short sourceProto, int channelId) {
		//logger.info("UponPrepareMsg request {} {}", prepare.getInstance(), channelId);
		if(joinedInstance!=-1 && prepare.getInstance()>=joinedInstance) {

			PaxosInstance p= paxosInstances.get(prepare.getInstance());
			if(p==null) {
				p = new PaxosInstance(from, membership,idx);
				paxosInstances.put(prepare.getInstance(), p);
			}

			int sn=prepare.getProposer_Seq();
			if(sn > p.getHighest_prepare()) {
				//logger.info("Prepare msg Dentro if {} {} {}", prepare.getInstance(), from, myself);
				p.setHighest_prepare(sn);
				PrepareOkMessage prepOkMsg=new PrepareOkMessage(from,sn,p.getHighest_accept(),p.getHighest_Op(),prepare.getInstance());
				sendMessage(prepOkMsg, from);
				//logger.info("Entrou no Prepare if + sn:"+ sn + "  p.getHighest_prepare(): "+ p.getHighest_prepare() + " in instance " + prepare.getInstance()+ " and getDecided: "+p.getDecided());
			}
			paxosInstances.put(prepare.getInstance(), p);
		}
	}

	private void uponPrepareOkMessage(PrepareOkMessage prepareOk,Host from, short sourceProto, int channelId) {
		//logger.info("UponPrepareOKMsg request {}", prepareOk.getInstance());
		if(joinedInstance!=-1 && prepareOk.getInstance()>=joinedInstance) {


			int sn= prepareOk.getProposer_Seq();
			int na= prepareOk.gethighAccept();
			PaxosOperation va= prepareOk.getHighOp();
			PaxosInstance p= paxosInstances.get(prepareOk.getInstance());

			//logger.info("Dentro prepareOk IF p.getProposer_seq() {} sn {} in instance {}", p.getProposer_seq(), sn,prepareOk.getInstance());

			if(p.getProposer_seq()==sn) {

				p.add_To_Prepare_ok_set(na, va);
				//logger.info("Adicionou ao prepareOkSet: na- "+na+" ."+ " com tamanho "+ p.getSize_Prepare_ok_set());
				if(p.getSize_Prepare_ok_set() >= (p.getMembership().size()/2)+1) {

					Entry<Integer, ArrayList<PaxosOperation>> highestEntry= p.getHighest_Of_Prepare_ok_set();
					if(highestEntry!=null && highestEntry.getValue()!=null && highestEntry.getValue().get(0)!=null) {
						PaxosOperation op= highestEntry.getValue().get(0);
						p.setProposer_op(op.getOp(),op.getOp_Id());
					}
					AcceptMessage acceptMsg;
					for(Host member: p.getMembership()) {
						acceptMsg= new AcceptMessage(member,p.getProposer_seq(),p.getProposer_op(),prepareOk.getInstance(),p.getMembership());
						sendMessage(acceptMsg, member);
						//logger.info("Entrou no If do prepareOkSet");
					}
					p.setPrepare_ok_set(new TreeMap<>() );
					this.cancelTimer(p.getTimer());
					long paxosTimerId= setupTimer(new PaxosTimer(prepareOk.getInstance()), paxosTimeutTime);
					p.setTimer(paxosTimerId);
				}
			}
			paxosInstances.put(prepareOk.getInstance(), p);
		}

	}

	private void uponAcceptMessage(AcceptMessage accept,Host from, short sourceProto, int channelId) {
		//logger.info("UponAcceptMsg request {}", accept.getInstance());
		if(joinedInstance!=-1 && accept.getInstance()>=joinedInstance) {
			int sn= accept.getSeq();
			PaxosOperation op = accept.getOp();

			PaxosInstance p= paxosInstances.get(accept.getInstance());
			if(p==null) {
				p = new PaxosInstance(from, accept.getMembership(),idx);
				paxosInstances.put(accept.getInstance(), p);
			}

			p.setMembership(accept.getMembership());
			//logger.info("Dentro accept IF {} {}", p.getHighest_prepare(), sn);
			if(sn >= p.getHighest_prepare()) {
				p.setHighest_prepare(sn);
				p.setHighest_accept(sn);
				p.setHighest_Op(op);
				AcceptOkMessage acceptOkMsg;
				for(Host member: p.getMembership()) {
					acceptOkMsg=new AcceptOkMessage(member,sn,p.getHighest_Op(),accept.getInstance());
					sendMessage(acceptOkMsg, member);
				}
			}
			paxosInstances.put(accept.getInstance(), p);
		}
	}



	private void uponAcceptOkMessage(AcceptOkMessage acceptOk,Host from, short sourceProto, int channelId) {
		//logger.info("UponAcceptOKMsg request {}", acceptOk.getInstance());

		if(joinedInstance!=-1 && acceptOk.getInstance()>=joinedInstance) {
			int sn= acceptOk.getSeq();
			PaxosOperation op= acceptOk.getHighOp();


			PaxosInstance p= paxosInstances.get(acceptOk.getInstance());
			if(p==null) {
				p = new PaxosInstance(from, membership,idx);
				paxosInstances.put(acceptOk.getInstance(), p);
			}


			//logger.info("AcceptOkMsg 1 {}", p);
			//logger.info("AcceptOkMsg 2{}", p.getAccept_ok_set());
			//logger.info("AcceptOkMsg 3{}", p.getAccept_ok_set().firstEntry());

			Entry<Integer, ArrayList<PaxosOperation>> entry = p.getAccept_ok_set().firstEntry();


			if(entry==null || (entry.getKey()==sn && op.equals(entry.getValue().get(0))) ){
				p.add_To_Accept_ok_set(sn, op);
			}else if(entry.getKey()<sn) {
				TreeMap<Integer, ArrayList<PaxosOperation>> newSet = new TreeMap<>();
				p.setAccept_ok_set(newSet);
				p.add_To_Accept_ok_set(sn, op);
			}else{
				//logger.info("Entrou no AcceptOk e n fez nada .");
			}

			if( p.getDecided()==null && p.getSize_Accept_ok_set() >= (p.getMembership().size()/2)+1 ) {
				p.setDecided(op);
				//logger.info("Decidiu " + " in instance "+ acceptOk.getInstance() +" id da op "+ op.getOp_Id());
				triggerNotification(new DecidedNotification(acceptOk.getInstance(), op.getOp_Id(), op.getOp()));

				//logger.info("DECIDIU com: sn"+ sn + " p.getProposer_seq() "+ p.getProposer_seq()+" com opId: "+ op.getOp_Id() );
				if(sn==p.getProposer_seq()) {
					//logger.info("Cancela Timer: "+p.getTimer() + " in instance: "+acceptOk.getInstance());
					this.cancelTimer(p.getTimer());
				}
			}
			paxosInstances.put(acceptOk.getInstance(), p);
		}

	}

	private void uponPaxosTimer(PaxosTimer pTimer, long timerId) {
		int instN=pTimer.getInstance();
		PaxosInstance p = paxosInstances.get(instN);
		//logger.info("Timeout do timer: "+p.getTimer());
		//logger.info("Paxos Timeout with instance number "+instN+" and op: "+ p.getHighest_Op().getOp_Id() );

		if(p.getDecided()==null) {
			//logger.info("Timeout in instance "+ instN);
			p.setProposer_seq(p.getProposer_seq()+p.getMembership().size());
			PrepareMessage prepMsg;
			for(Host member: p.getMembership()) {
				prepMsg= new PrepareMessage(member,p.getProposer_seq(),instN);
				sendMessage(prepMsg, member);
			}
			p.setPrepare_ok_set(new TreeMap<>());
			long paxosTimerId = setupTimer(new PaxosTimer(instN), paxosTimeutTime);
			p.setTimer(paxosTimerId);
		}else{
			//logger.info("Timeout with getDecided " + p.getDecided().getOp_Id() + " in instance "+ instN);
		}
		paxosInstances.put(instN, p);
	}


	private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
		//logger.info("Received " + request);
		//The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
		//You should probably take it into account while doing whatever you do here.
		if(request.getReplica()==myself) {
			//logger.info("Received " + request+" of myself in instance: "+request.getInstance());
			joinedInstance=request.getInstance();
		}

		membership.add(request.getReplica());
	}

	private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
		//logger.info("Received " + request);
		if(request.getReplica()==myself) {
			//logger.info("Received " + request+" of myself in instance:"+request.getInstance());
			joinedInstance=-1;
		}

		//The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
		//You should probably take it into account while doing whatever you do here.

		membership.remove(request.getReplica());
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		//logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

}
