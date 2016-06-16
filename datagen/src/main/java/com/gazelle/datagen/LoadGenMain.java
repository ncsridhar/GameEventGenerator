package com.gazelle.datagen;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import com.collector.gazelle.dto.AuditInfo;
import com.collector.gazelle.dto.GzEvent;
import com.collector.gazelle.dto.RoutingInfo;
import com.collector.gazelle.kafka.GzQueue;
import com.collector.gazelle.kafka.GzQueueKafkaImpl;
import com.google.gson.Gson;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;

public class LoadGenMain {

	public static Long getRandom(Long aStart, Long aEnd, Random aRandom) {
		if (aStart > aEnd) {
			throw new IllegalArgumentException("Start cannot exceed End.");
		}
		// get the range, casting to long to avoid overflow problems
		long range = (long) aEnd - (long) aStart + 1;
		// compute a fraction of the range, 0 <= frac < range
		long fraction = (long) (range * aRandom.nextDouble());
		long randomNumber = (int) (fraction + aStart);
		return (Long) randomNumber;
	}

	public static class Msg implements Serializable {
		private static final long serialVersionUID = 1L;
		String userId;

		public Msg(String userId) {
			super();
			this.userId = userId;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

	}

	public static class SessionMsg extends Msg {

		String sessionId;
		String event;
		Long time;

		public SessionMsg(String userId, String sessionId, String event,
				Long time) {
			super(userId);
			this.sessionId = sessionId;
			this.event = event;
			this.time = time;
		}

		public String getSessionId() {
			return sessionId;
		}

		public void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}

		public String getEvent() {
			return event;
		}

		public void setEvent(String event) {
			this.event = event;
		}

		public Long getTime() {
			return time;
		}

		public void setTime(Long time) {
			this.time = time;
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return "userid: " + getUserId() + "; sessionID: " + sessionId + "; event: " + event + " ; time: " + time;
		}
	}

	public static class LoadWorker extends UntypedActor {

		Random r = new Random();
		ActorRef senders = null;

		public LoadWorker(ActorRef senders) {
			this.senders = senders;
		}

		@Override
		public void onReceive(Object m) throws Exception {
			// TODO Auto-generated method stub
			if (m instanceof Msg) {
				String userId = ((Msg) m).getUserId();
				System.out.println("received this user id: " + userId);
				Long events = getRandom(1L, 100L, r);
				String guid = UUID.randomUUID().toString();
				SessionMsg msg = new SessionMsg(userId,guid,"START_GAME", System.currentTimeMillis());
				senders.tell(msg);
				for(int i=1; i<=events; i++){
					// (int)(Math.random() * maximum)
					Integer randomInt = (int)(Math.random() * events);
					msg = new SessionMsg(userId,guid,"EVENT_"+randomInt, System.currentTimeMillis() + i*2000);
					senders.tell(msg);
				}
				
				msg = new SessionMsg(userId,guid,"END_GAME", System.currentTimeMillis() + events*2000 + 2000);
				senders.tell(msg);
			}

		}

	}

	public static class Sender extends UntypedActor {

		private GzQueue queue = null;
		Gson gson = new Gson();
		
		@Override
		public void preStart() {
			// TODO Auto-generated method stub
			super.preStart();
			queue = new GzQueueKafkaImpl();
		}
		@Override
		public void onReceive(Object m) throws Exception {
			// TODO Auto-generated method stub
			if (m instanceof SessionMsg){
				SessionMsg msg = (SessionMsg) m;
				//System.out.println(msg.toString());
				queue.put(getGzEvent(msg), false);
			}
		}
		
		private GzEvent getGzEvent(SessionMsg msg){
			GzEvent e = new GzEvent();
			RoutingInfo r = new RoutingInfo("x", "SESSION");
			AuditInfo a = new AuditInfo("sridhar", 100L);
			
			e.setAuditInfo(a);
			e.setRoutingInfo(r);
			e.setJsonPayload(gson.toJson(msg));
			
			return e;
		}

	}

	public static class Master extends UntypedActor {

		private int perSec = 0;
		ActorRef listener = null;
		ActorRef workers = null;
		ActorRef senders = null;

		public Master(int perSec, ActorRef listener) {
			this.perSec = perSec;
			this.listener = listener;
		}

		public void preStart() {

			senders = getContext().actorOf(
					new Props(Sender.class)
							.withRouter(new RoundRobinRouter(10)));

			workers = getContext().actorOf(new Props(new UntypedActorFactory() {
				public UntypedActor create() {
					return new LoadWorker(senders);
				}
			}).withRouter(new RoundRobinRouter(10)));

		}

		@Override
		public void onReceive(Object m) throws Exception {

			if (m instanceof String) {
				String msg = (String) m;
				if (msg.equals("START")) {
					generateLoad(workers);
				} else if (msg.equals("STOP")) {
					listener.tell("SHUTDOWN");
				}
			} else {
				System.out
						.println("Unknonw command to master, so shutting down");
				listener.tell("SHUTDOWN");
			}
		}

		private void generateLoad(ActorRef workers) {
			// TODO Auto-generated method stub
			Random r = new Random();
			for (int i = 0; i < perSec; i++) {
				Long id = getRandom(1L, 100000000L, r);
				String ids = Long.toString(id);
				workers.tell(new Msg(ids));
			}
		}

	}

	public static class Listener extends UntypedActor {
		public void onReceive(Object message) {
			if (message instanceof String) {
				System.out.println("System shutdown");
				getContext().system().shutdown();
			} else {
				unhandled(message);
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		int noOfSecs = 5;
		final int perSec = 1000;

		ActorSystem system = ActorSystem.create("LoadGen-Actor-system");

		final ActorRef listener = system.actorOf(new Props(Listener.class),
				"listener");

		ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
			public UntypedActor create() {
				return new Master(perSec, listener);
			}
		}), "master");

		for (int i = 0; i < noOfSecs; i++) {
			master.tell("START");
			Thread.sleep(1000);
		}

		listener.tell("STOP");

	}

}
