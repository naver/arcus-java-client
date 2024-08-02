package net.spy.memcached;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class DefaultArcusConnectionFactoryTest {

  private final static Set<Class<?>> PRIMITIVE_TYPES = new HashSet<>();
  private final static Set<String> OBJECT_METHODS = new HashSet<>();

  static {
    PRIMITIVE_TYPES.add(Boolean.class);
    PRIMITIVE_TYPES.add(Byte.class);
    PRIMITIVE_TYPES.add(Short.class);
    PRIMITIVE_TYPES.add(Integer.class);
    PRIMITIVE_TYPES.add(Long.class);
    PRIMITIVE_TYPES.add(Float.class);
    PRIMITIVE_TYPES.add(Double.class);
    PRIMITIVE_TYPES.add(Character.class);
    PRIMITIVE_TYPES.add(String.class);

    for (Method method : Object.class.getMethods()) {
      OBJECT_METHODS.add(method.getName());
    }
  }

  @Test
  public void testCompareToDefaultConnectionFactory()
          throws InvocationTargetException, IllegalAccessException {

    ConnectionFactory a = new DefaultConnectionFactory();
    ConnectionFactory b = new DefaultArcusConnectionFactory();

    Set<String> methodsReturnDifferent = new HashSet<>();
    methodsReturnDifferent.add("getFailureMode");
    methodsReturnDifferent.add("isDaemon");
    methodsReturnDifferent.add("getMaxReconnectDelay");
    methodsReturnDifferent.add("getHashAlg");
    methodsReturnDifferent.add("getTimeoutExceptionThreshold");
    methodsReturnDifferent.add("getTimeoutDurationThreshold");
    methodsReturnDifferent.add("getFrontCacheName");

    compareConnectionFactory(a, b, methodsReturnDifferent);
  }

  @Test
  public void testCompareToConnectionFactoryBuilder()
          throws InvocationTargetException, IllegalAccessException {

    ConnectionFactory a = new ConnectionFactoryBuilder().build();
    ConnectionFactory b = new DefaultArcusConnectionFactory(a.getFrontCacheName());

    compareConnectionFactory(a, b, new HashSet<>());
  }

  private void compareConnectionFactory(ConnectionFactory a,
                                        ConnectionFactory b,
                                        Set<String> methodsReturnDifferent)
          throws InvocationTargetException, IllegalAccessException {

    Map<String, Method> aMethods = getMethodsToTest(a);
    Map<String, Method> bMethods = getMethodsToTest(b);

    assertFalse(aMethods.isEmpty());
    assertFalse(bMethods.isEmpty());

    for (Map.Entry<String, Method> aEntry : aMethods.entrySet()) {
      String methodName = aEntry.getKey();
      if (!bMethods.containsKey(methodName)) {
        continue;
      }

      Object aField = aEntry.getValue().invoke(a);
      Object bField = bMethods.get(methodName).invoke(b);

      if (methodsReturnDifferent.contains(methodName)) {
        assertNotEquals(aField, bField);
        continue;
      }

      if (aField == null) {
        assertNull(bField);
        continue;
      }

      Class<?> aClass = aField.getClass();
      if (PRIMITIVE_TYPES.contains(aClass)) {
        assertEquals(aField, bField);
      } else {
        assertEquals(aClass, bField.getClass());
      }
    }
  }

  private Map<String, Method> getMethodsToTest(Object c) {
    Map<String, Method> methods = new HashMap<>();
    for (Method method : c.getClass().getMethods()) {
      int modifiers = method.getModifiers();
      if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {
        continue;
      }
      if (method.getParameterCount() > 0) {
        continue;
      }
      if (method.getReturnType().equals(Void.TYPE)) {
        continue;
      }
      if (OBJECT_METHODS.contains(method.getName())) {
        continue;
      }

      methods.put(method.getName(), method);
    }

    return methods;
  }
}
