package edu.uiuc.dprg.morphous;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Primitives;

public class Util {
	private static Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

	public static String toStringCF(ColumnFamily data) {
		StringBuilder builder = new StringBuilder();
		try {
	    	for (Column column : data.getSortedColumns()) {
	    		builder
	    		.append(", ")
	    		.append(ByteBufferUtil.string(column.name()))
	    		.append(" : ")
	    		.append(ByteBufferUtil.string(column.value()));
	    	}
			logger.info("{}", builder);
		} catch (CharacterCodingException e) {
			logger.warn("Unable to print Column Family");
		}
		return builder.toString();
	}

	@SuppressWarnings("unchecked")
	public static <T> T getPrivateFieldWithReflection(Object object, String fieldName) {
		Field field;
		T result;
		try {
			field = object.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
			result = (T) field.get(object);
		} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException e) {
			// This should not happen
			throw new RuntimeException(String.format("Reflection exception trying to unwrap a private field %s from %s", fieldName, object), e);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static <T> T invokePrivateMethodWithReflection(Object object, String methodName, Object... parameters) {
		Method method = null;
		T result;
		try {
			List<Class<?>> parameterTypes = new ArrayList<>();
			if (parameters != null) {
				for (Object param : parameters) {
					parameterTypes.add(param.getClass());
				}				
			}
			
			
			try {
				method = object.getClass().getDeclaredMethod(methodName, parameterTypes.toArray(new Class<?>[]{}));	
			} catch(NoSuchMethodException e) {
				outerloop:
				for (Method curMethod : object.getClass().getDeclaredMethods()) {
					if (curMethod.getName().equals(methodName) && parameterTypes.size() == curMethod.getParameterTypes().length) {
						for (int i = 0; i < parameterTypes.size(); i++) {
							if (!parameterTypes.get(i).equals(Primitives.wrap(curMethod.getParameterTypes()[i]))) {
								continue outerloop; 
							}
						}
						method = curMethod;
						break;
					}
				}
				if (method == null) {
					throw e;
				}
			}
			
			method.setAccessible(true);
			result = (T) method.invoke(object, parameters);
		} catch (InvocationTargetException e) {
			// Exception from the function being called.
			throw new RuntimeException(e);
		} catch (IllegalArgumentException | IllegalAccessException | NoSuchMethodException | SecurityException e) {
			// This should not happen
			throw new RuntimeException(String.format("Reflection exception trying to unwrap a private method %s from %s", methodName, object), e);
		}
		return result;
	}

	public static ByteBuffer getColumnNameByteBuffer(String columnName) {
		ByteBuffer rawBb = ByteBufferUtil.bytes(columnName);
		return getColumnNameByteBuffer(rawBb);
	}
	
	public static ByteBuffer getColumnNameByteBuffer(ByteBuffer columnNameByteBuffer) {
		ByteBuffer bb = ByteBuffer.allocate(TypeSizes.NATIVE.sizeofWithShortLength(columnNameByteBuffer) + 1);
		ByteBufferUtil.writeShortLength(bb, columnNameByteBuffer.remaining());
		bb.put(columnNameByteBuffer);
		bb.put((byte) 0);
		bb.rewind();
		return bb;
	}

	@SuppressWarnings("rawtypes")
	public static Collection<Range<Token>> getNthRangesForLocalNode(String keyspace, int n) {
	    AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
	    Collection<Range<Token>> nthRanges = new HashSet<Range<Token>>();
	    
	    TokenMetadata metadata = ((TokenMetadata) getPrivateFieldWithReflection(StorageService.instance, "tokenMetadata")).cloneOnlyTokenMap();
	                    
	    for (Token token : metadata.sortedTokens())
	    {
	        List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
	        if (endpoints.size() >= n && endpoints.get(n - 1).equals(FBUtilities.getBroadcastAddress()))
	            nthRanges.add(new Range<Token>(metadata.getPredecessor(token), token));
	    }
	    return nthRanges;
	}

	public static Cassandra.Client getClient() throws TTransportException
	{
	    TTransport tr = new TFramedTransport(new TSocket("localhost", DatabaseDescriptor.getRpcPort()));
	    TProtocol proto = new TBinaryProtocol(tr);
	    Cassandra.Client client = new Cassandra.Client(proto);
	    tr.open();
	    return client;
	}

	public static CqlResult executeCql3Statement(String statement) {
		try {
			Cassandra.Client client = getClient();
			return client.execute_cql3_query(ByteBufferUtil.bytes(statement), Compression.NONE, ConsistencyLevel.QUORUM);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

}
