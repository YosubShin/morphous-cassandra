package edu.uiuc.dprg.morphous;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationVerbHandler;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class MorphousTaskMessageSender {
	private static final Logger logger = LoggerFactory.getLogger(MorphousTaskMessageSender.class);
	private static MorphousTaskMessageSender instance = new MorphousTaskMessageSender();
	
	static long timeoutInMillis = 1000 * 60 * 30;
	HashMap<String, Map<InetAddress, MorphousTaskResponse>> messageResponseMap = new HashMap<>();
	HashMap<String, MorphousTask> morphousTaskMap = new HashMap<>();
		
	private MorphousTaskMessageSender() {	
	}
	
	public static MorphousTaskMessageSender instance() {
		return instance;
	}
	
	public void endpointHasResponded(MorphousTaskResponse taskResponse, InetAddress from) {
		logger.debug("EndpointHasResponded() was invoked with response {}, and from {}", taskResponse, from);
		synchronized(messageResponseMap) {
			Map<InetAddress, MorphousTaskResponse> messageResponses = messageResponseMap.get(taskResponse.taskUuid);
			if (messageResponses == null || !messageResponses.containsKey(from)) {
				logger.warn("The MorphousTask for response {} does not exists probably due to timeout", taskResponse);
				return;
			}
			messageResponses.put(from, taskResponse);
			logger.debug("messageResponses after endpoint {} has responded :{}", from, messageResponses);
		}
	}
	
	public void sendMorphousTaskToAllEndpoints(MorphousTask task) {
		logger.debug("Sending MorphousTask {} to all endpoints", task);
		MessageOut<MorphousTask> message = new MessageOut<>(Verb.MORPHOUS_TASK, task, MorphousTask.serializer);
		sendMessageToAllEndpoints(message);
	}
	
	public void sendMessageToAllEndpoints(MessageOut<MorphousTask> message) {
		long taskStartedAt = System.currentTimeMillis();
		IAsyncCallback<MorphousTaskResponse> callback = new IAsyncCallback<MorphousTaskMessageSender.MorphousTaskResponse>() {
			
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
		
		final MorphousTask task = message.payload;
		synchronized(messageResponseMap) {
			HashMap<InetAddress, MorphousTaskResponse> messageResponses = new HashMap<>();
			messageResponseMap.put(task.taskUuid, messageResponses);
			morphousTaskMap.put(task.taskUuid, task);
			
			for (InetAddress dest : Gossiper.instance.getLiveMembers()) {
				logger.debug("Sending MorphousTask message {} to destination {}", message, dest);
				MessagingService.instance().sendRR(message, dest, callback, timeoutInMillis);
				messageResponses.put(dest, new MorphousTaskResponse());
			}	
		}
		
		logger.debug("About to enter while loop");
		// Should wait for response to comeback till the timeout is over
		while (System.currentTimeMillis() < taskStartedAt + timeoutInMillis) {
			logger.debug("Entered while loop");
			synchronized(messageResponseMap) {
				if(isMorphousTaskOver(task.taskUuid)) {
					logger.info("Morphous Task ended in {} milliseconds", System.currentTimeMillis() - taskStartedAt);
					final Map<InetAddress, MorphousTaskResponse> responses = messageResponseMap.remove(task.taskUuid);
					morphousTaskMap.remove(task.taskUuid);
					
					// Do this because calling taskIsDone() synchronously will cause it to block all callbacks (deadlock until it times out)
					new Thread(new Runnable() {
						@Override
						public void run() {
							task.taskIsDone(responses);
						}
					}).start();
					return;
				} else {
					handleNodeFailures(task.taskUuid);
				}
			}
						
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted while wating for the task to finish", e);
			}
		}
		
		// If the task is still sitting there, then it must have timed out!
		synchronized(messageResponseMap) {
			logger.warn("Morphous Task {} timed out", task);
			messageResponseMap.remove(task.taskUuid);
			morphousTaskMap.remove(task.taskUuid);	
		}
		
	}

	private void handleNodeFailures(String taskUuid) {
		MorphousTask task = morphousTaskMap.get(taskUuid);
		if (task == null || !messageResponseMap.containsKey(task.taskUuid)) {
			return;
		} else {
			for (Entry<InetAddress, MorphousTaskResponse> entry : messageResponseMap.get(task.taskUuid).entrySet()) {
				MorphousTaskResponse value = entry.getValue();
				if (value.status != MorphousTaskResponseStatus.SUCCESSFUL && value.status != MorphousTaskResponseStatus.NODE_FAILED && !FailureDetector.instance.isAlive(entry.getKey())) {
					logger.debug("The node {} failed during reconfiguration task {}.", entry.getKey(), value);
					value.status = MorphousTaskResponseStatus.NODE_FAILED;
				}
			}
		}
	}

	public boolean isMorphousTaskOver(String taskUuid) {
		MorphousTask task = morphousTaskMap.get(taskUuid);
		if (task == null || !messageResponseMap.containsKey(task.taskUuid)) {
			return true;
		} else {
			for (Entry<InetAddress, MorphousTaskResponse> entry : messageResponseMap.get(task.taskUuid).entrySet()) {
				MorphousTaskResponse value = entry.getValue();
				if (value.status != MorphousTaskResponseStatus.SUCCESSFUL && value.status != MorphousTaskResponseStatus.NODE_FAILED) {
					logger.debug("Morphous Task is not over due to node : {}, task {}", entry.getKey(), value);
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
		public MorphousTaskCallback callback;
		public Long taskStartedAtInMicro;
        public boolean autoCompactionOn;
		public int numConcurrentRowMutationSenderThreads;
		
		public static final IVersionedSerializer<MorphousTask> serializer = new IVersionedSerializer<MorphousTaskMessageSender.MorphousTask>() {
			
			@Override
			public long serializedSize(MorphousTask t, int version) {
				long size = 0;
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.taskUuid));
				size += TypeSizes.NATIVE.sizeof(t.taskType.ordinal());
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.keyspace));
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.columnFamily));
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.newPartitionKey));
				size += TypeSizes.NATIVE.sizeof(t.taskStartedAtInMicro);
                size += TypeSizes.NATIVE.sizeof(t.autoCompactionOn);
				size += TypeSizes.NATIVE.sizeof(t.numConcurrentRowMutationSenderThreads);
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
				out.writeLong(t.taskStartedAtInMicro);
                out.writeBoolean(t.autoCompactionOn);
				out.writeInt(t.numConcurrentRowMutationSenderThreads);
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
				result.taskStartedAtInMicro = in.readLong();
                result.autoCompactionOn = in.readBoolean();
				result.numConcurrentRowMutationSenderThreads = in.readInt();
				
				logger.debug("deserialized MorphousTask : {}", result);
				return result;
			}
		};
		
		public MorphousTask() {
			this.taskUuid = UUID.randomUUID().toString();
		}
		
		public void taskIsDone(Map<InetAddress, MorphousTaskResponse> responses) {
			for (Entry<InetAddress, MorphousTaskResponse> entry : responses.entrySet()) {
				MorphousTaskResponse response = entry.getValue();
				assert response.status == MorphousTaskResponseStatus.SUCCESSFUL || response.status == MorphousTaskResponseStatus.NODE_FAILED : "MorphousTaskResponse from " + entry.getKey() + " is not successful";
			}
			if (this.callback != null) {
				logger.info("MorphousTask {} is done, now executing callback", this);
				try {
					callback.callback(this, responses);	
				} catch (Exception e) {
					logger.error("Execption while executing callback on MorphousTask {}, with exception {}", this, e);
					throw new MorphousException("Error while executing callback on MorphousTask", e);
				}
			} else {
				logger.info("MorphousTask {} is done", this);		
			}
		}

		@Override
		public String toString() {
			return "MorphousTask{" +
					"taskUuid='" + taskUuid + '\'' +
					", taskType=" + taskType +
					", keyspace='" + keyspace + '\'' +
					", columnFamily='" + columnFamily + '\'' +
					", newPartitionKey='" + newPartitionKey + '\'' +
					", callback=" + callback +
					", taskStartedAtInMicro=" + taskStartedAtInMicro +
					", autoCompactionOn=" + autoCompactionOn +
					", numConcurrentRowMutationSenderThreads=" + numConcurrentRowMutationSenderThreads +
					'}';
		}
	}
	
	public enum MorphousTaskType {
        COMPACT,
		INSERT,
		CATCH_UP,
		ATOMIC_SWITCH;
	}
	
	public static class MorphousTaskResponse {
		public String taskUuid;
		public MorphousTaskResponseStatus status = MorphousTaskResponseStatus.NULL;
		public String message = "";
		public static final IVersionedSerializer<MorphousTaskResponse> serializer = new IVersionedSerializer<MorphousTaskMessageSender.MorphousTaskResponse>() {
			
			@Override
			public long serializedSize(MorphousTaskResponse t, int version) {
				long size = 0;
				size += TypeSizes.NATIVE.sizeofWithShortLength(ByteBufferUtil.bytes(t.taskUuid));
				size += TypeSizes.NATIVE.sizeof(t.status.ordinal());
				size += TypeSizes.NATIVE.sizeofWithLength(ByteBufferUtil.bytes(t.message));
				return size;
			}
			
			@Override
			public void serialize(MorphousTaskResponse t, DataOutput out, int version)
					throws IOException {
				
				ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(t.taskUuid), out);
				out.writeInt(t.status.ordinal());
				ByteBufferUtil.writeWithLength(ByteBufferUtil.bytes(t.message), out);
			}
			
			@Override
			public MorphousTaskResponse deserialize(DataInput in, int version)
					throws IOException {
				logger.debug("Deserializing MorphousTaskResponse");
				MorphousTaskResponse result = new MorphousTaskResponse();
				result.taskUuid = ByteBufferUtil.string(ByteBufferUtil.readWithShortLength(in));
				result.status = MorphousTaskResponseStatus.values()[in.readInt()];
				result.message = ByteBufferUtil.string(ByteBufferUtil.readWithLength(in));
				
				logger.debug("deserialized MorphousTaskResponse : {}", result);
				return result;
			}
		};
		@Override
		public String toString() {
			return "MorphousTaskResponse [taskUuid=" + taskUuid + ", status="
					+ status + ", message=" + message + "]";
		}

	}
	
	public enum MorphousTaskResponseStatus {
		SUCCESSFUL,
		FAILED,
		NODE_FAILED,
		NULL;
	}
	
	public interface MorphousTaskCallback {
		void callback(MorphousTask task, Map<InetAddress, MorphousTaskResponse> responses);
	}
	
	public static class MorphousVerbHandler implements IVerbHandler<MorphousTask> {
		public static final Map<MorphousTaskType, MorphousTaskHandler> taskHandlers;
		static {
			taskHandlers = new HashMap<MorphousTaskMessageSender.MorphousTaskType, MorphousTaskHandler>();
            taskHandlers.put(MorphousTaskType.COMPACT, new CompactMorphousTaskHandler());
			taskHandlers.put(MorphousTaskType.INSERT, new InsertMorphousTaskHandler());
			taskHandlers.put(MorphousTaskType.ATOMIC_SWITCH, new AtomicSwitchMorphousTaskHandler());
			taskHandlers.put(MorphousTaskType.CATCH_UP, new CatchupMorphousTaskHandler());
		}

		@Override
		public void doVerb(MessageIn<MorphousTask> message, int id) {
			long startAt = System.currentTimeMillis();
			logger.info("MorphousTask message with id {} Received : {}, and payload : {}", id, message, message.payload);
			MorphousTask task = message.payload;			
			MorphousTaskHandler handler = taskHandlers.get(task.taskType);
			MorphousTaskResponse taskResponse = null;
			try {
				if (handler == null) {
					throw new MorphousException("Handler for the Morphous Task does not exists!");
				}
				taskResponse = handler.handle(task);	
			} catch (Exception e) {
				logger.error("MorphousTask Handling failed with Exception {}.", e);
				taskResponse = new MorphousTaskResponse();
				taskResponse.taskUuid = task.taskUuid;
				taskResponse.status = MorphousTaskResponseStatus.FAILED;
				taskResponse.message = Throwables.getStackTraceAsString(e);
			}
					
			logger.debug("Finished executing MorphousTask {} in {} ms.", task, System.currentTimeMillis() - startAt);
			
			MessageOut<MorphousTaskResponse> responseMessage = new MessageOut<MorphousTaskResponse>(MessagingService.Verb.REQUEST_RESPONSE, taskResponse, MorphousTaskResponse.serializer);
			
			logger.debug("Sending MorphousTaskResponse reply to {}, with message {}", message.from, responseMessage);
			MessagingService.instance().sendReply(responseMessage, id, message.from);
		}
		
	}

	public static class MorphusMutationVerbHandler extends RowMutationVerbHandler {
		@Override
		public void doVerb(MessageIn<RowMutation> message, int id) {
			TraceState state = Tracing.instance.initializeFromMessage(message);
			if (state != null) {
				state.trace("Handling Morphus Mutation from {}", message.from);
			}
			super.doVerb(message, id);
		}
	}
	
	
}
