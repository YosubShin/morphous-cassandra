package edu.uiuc.dprg.morphous;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSender {
	private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);
	private static MessageSender instance = new MessageSender();
	
	static long timeoutInMillis = 1000 * 60 * 5; 
	ConcurrentHashMap<String, Map<InetAddress, Boolean>> messageResponseMap = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, MorphousTask> morphousTaskMap = new ConcurrentHashMap<String, MessageSender.MorphousTask>();
	
	private MessageSender() {	
	}
	
	public static MessageSender instance() {
		return instance;
	}
	
	public void endpointHasResponded(MorphousTaskResponse taskResponse, InetAddress from) {
		synchronized(messageResponseMap) {
			synchronized (morphousTaskMap) {
				Map<InetAddress, Boolean> messageResponses = messageResponseMap.get(taskResponse.taskUuid);
				if (messageResponses == null || !messageResponses.containsKey(from)) {
					logger.warn("The MorphousTask for response {} does not exists probably due to timeout", taskResponse);
					return;
				}
				messageResponses.put(from, true);
			}
		}
	}
	
	public void sendMorphousTaskToAllEndpoints(MorphousTask task) {
		logger.debug("Sending MorphousTask {} to all endpoints", task);
		MessageOut<MorphousTask> message = new MessageOut<>(Verb.MORPHOUS_TASK, task, MorphousTask.serializer);
		sendMessageToAllEndpoints(message);
	}
	
	public void sendMessageToAllEndpoints(MessageOut<MorphousTask> message) {
		long taskStartedAt = System.currentTimeMillis();
		IAsyncCallback<MorphousTaskResponse> callback = new IAsyncCallback<MessageSender.MorphousTaskResponse>() {
			
			@Override
			public void response(MessageIn<MorphousTaskResponse> msg) {
				logger.info("Message with id {} is responded by {}", msg.payload, msg.from);
				endpointHasResponded(msg.payload, msg.from);
			}
			
			@Override
			public boolean isLatencyForSnitch() {
				return false;
			}
		}; 
		
		MorphousTask task = message.payload;
		HashMap<InetAddress, Boolean> messageResponses = new HashMap<>();
		messageResponseMap.put(task.taskUuid, messageResponses);
		morphousTaskMap.put(task.taskUuid, task);
		
		for (InetAddress dest : Gossiper.instance.getLiveMembers()) {
			logger.debug("Sending MorphousTask message {} to destination {}", message, dest);
			MessagingService.instance().sendRR(message, dest, callback, timeoutInMillis);
			messageResponses.put(dest, false);
		}
		
		// Should wait for response to comeback till the timeout is over
		while (System.currentTimeMillis() < taskStartedAt + timeoutInMillis) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted while wating for the task to finish", e);
			}
			synchronized(messageResponseMap) {
				synchronized (morphousTaskMap) {
					if(isMorphousTaskOver(task.taskUuid)) {
						logger.info("Morphous Task ended in {} milliseconds", System.currentTimeMillis() - taskStartedAt);
						task.taskIsDone();
						messageResponseMap.remove(task.taskUuid);
						morphousTaskMap.remove(task.taskUuid);
						return;
					}
				}
			}
		}
		
		// If the task is still sitting there, then it must have timed out!
		synchronized(messageResponseMap) {
			synchronized (morphousTaskMap) {
				logger.warn("Morphous Task {} timed out", task);
				messageResponseMap.remove(task.taskUuid);
				morphousTaskMap.remove(task.taskUuid);
			}
		}
		
	}
	
	public boolean isMorphousTaskOver(String taskUuid) {
		MorphousTask task = morphousTaskMap.get(taskUuid);
		if (task == null || !messageResponseMap.containsKey(task.taskUuid)) {
			return true;
		} else {
			for (boolean value : messageResponseMap.get(task.taskUuid).values()) {
				if (!value) {
					return false;
				}
			}
		}
		return true;
	}
	
	public static class MorphousTask {
		public String taskUuid;
		public static final IVersionedSerializer<MorphousTask> serializer = new IVersionedSerializer<MessageSender.MorphousTask>() {
			
			@Override
			public long serializedSize(MorphousTask t, int version) {
				return t.taskUuid.getBytes().length;
			}
			
			@Override
			public void serialize(MorphousTask t, DataOutput out, int version)
					throws IOException {
				out.writeChars(t.taskUuid);
			}
			
			@Override
			public MorphousTask deserialize(DataInput in, int version)
					throws IOException {
				MorphousTask result = new MorphousTask();
				result.taskUuid = in.readLine();
				return result;
			}
		};
		
		public MorphousTask() {
			this.taskUuid = UUID.randomUUID().toString();
		}
		
		public void taskIsDone() {
			//TODO
			logger.info("Task {} is done", this);
		}
		
		@Override
		public String toString() {
			return "MorphousTask [taskUuid=" + taskUuid + "]";
		}
	}
	
	public static class MorphousTaskResponse {
		public String taskUuid;
	}
	
	public static class MorphousVerbHandler implements IVerbHandler<MorphousTask> {

		@Override
		public void doVerb(MessageIn<MorphousTask> message, int id) {
			// TODO Auto-generated method stub
			logger.info("MorphousTask message with id {} Received : {}", id, message);
		}
		
	}
	
	
}
