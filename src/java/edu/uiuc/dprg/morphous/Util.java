package edu.uiuc.dprg.morphous;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.ByteBufferUtil;
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

}
