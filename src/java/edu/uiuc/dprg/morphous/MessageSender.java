package edu.uiuc.dprg.morphous;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
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
				logger.info("MorphouTaskResponse message {} is responded by {}", msg.payload, msg.from);
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
		public MorphousTaskType taskType;
		public String keyspace;
		public String columnFamily;
		public String newPartitionKey;
		
		public static final IVersionedSerializer<MorphousTask> serializer = new IVersionedSerializer<MessageSender.MorphousTask>() {
			
			@Override
			public long serializedSize(MorphousTask t, int version) {
				long size = 0;
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.taskUuid));
				size += TypeSizes.NATIVE.sizeof(t.taskType.ordinal());
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.keyspace));
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.columnFamily));
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.newPartitionKey));
				return size;
			}
			
			@Override
			public void serialize(MorphousTask t, DataOutput out, int version)
					throws IOException {
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.taskUuid), out);
				out.writeInt(t.taskType.ordinal());
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.keyspace), out);
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.columnFamily), out);
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.newPartitionKey), out);
			}
			
			@Override
			public MorphousTask deserialize(DataInput in, int version)
					throws IOException {
				logger.debug("Deserializing MorphousTask");
				MorphousTask result = new MorphousTask();
				result.taskUuid = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				result.taskType = MorphousTaskType.values()[in.readInt()];
				result.keyspace = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				result.columnFamily = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				result.newPartitionKey = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				
				logger.debug("deserialized MorphousTask : {}", result);
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
			return "MorphousTask [taskUuid=" + taskUuid + ", taskType="
					+ taskType + ", keyspace=" + keyspace + ", columnFamily="
					+ columnFamily 
					+ ", newPartitionKey=" + newPartitionKey + "]";
		}
	}
	
	public enum MorphousTaskType {
		INSERT,
		CATCH_UP,
		ATOMIC_SWITCH;
	}
	
	public static class MorphousTaskResponse {
		public String taskUuid;
		public static final IVersionedSerializer<MorphousTaskResponse> serializer = new IVersionedSerializer<MessageSender.MorphousTaskResponse>() {
			
			@Override
			public long serializedSize(MorphousTaskResponse t, int version) {
				long size = 0;
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.taskUuid));
				return size;
			}
			
			@Override
			public void serialize(MorphousTaskResponse t, DataOutput out, int version)
					throws IOException {
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.taskUuid), out);
			}
			
			@Override
			public MorphousTaskResponse deserialize(DataInput in, int version)
					throws IOException {
				logger.debug("Deserializing MorphousTaskResponse");
				MorphousTaskResponse result = new MorphousTaskResponse();
				result.taskUuid = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				
				logger.debug("deserialized MorphousTaskResponse : {}", result);
				return result;
			}
		};
		@Override
		public String toString() {
			return "MorphousTaskResponse [taskUuid=" + taskUuid + "]";
		}
	}
	
	public static class MorphousVerbHandler implements IVerbHandler<MorphousTask> {

		@Override
		public void doVerb(MessageIn<MorphousTask> message, int id) {
			logger.info("MorphousTask message with id {} Received : {}, and payload : {}", id, message, message.payload);
			MorphousTask task = message.payload;			
			
			MorphousTaskResponse taskResponse = new MorphousTaskResponse();
			taskResponse.taskUuid = task.taskUuid;
			MessageOut<MorphousTaskResponse> responseMessage = new MessageOut<MorphousTaskResponse>(MessagingService.Verb.REQUEST_RESPONSE, taskResponse, MorphousTaskResponse.serializer);
			logger.debug("Sending MorphousTaskResponse reply to {}, with message {}", message.from, responseMessage);
			MessagingService.instance().sendReply(responseMessage, id, message.from);
		}
		
	}
	
	
}
