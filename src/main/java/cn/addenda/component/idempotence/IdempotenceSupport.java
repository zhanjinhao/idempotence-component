package cn.addenda.component.idempotence;

import cn.addenda.component.base.concurrent.SleepUtils;
import cn.addenda.component.base.datetime.TimeUnitUtils;
import cn.addenda.component.base.exception.ComponentServiceException;
import cn.addenda.component.base.exception.ServiceException;
import cn.addenda.component.base.exception.SystemException;
import cn.addenda.component.base.jackson.util.JacksonUtils;
import cn.addenda.component.base.lambda.TBiFunction;
import cn.addenda.component.base.lambda.TSupplier;
import cn.addenda.component.base.util.RetryUtils;
import cn.addenda.component.idempotence.statecenter.StateCenter;
import cn.addenda.component.spring.context.ValueResolverHelper;
import cn.addenda.component.spring.util.SpELUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author addenda
 * @since 2023/7/29 14:11
 */
@Slf4j
public class IdempotenceSupport implements EnvironmentAware, InitializingBean, ApplicationContextAware {

  /**
   * {@link EnableIdempotenceManagement#namespace()}
   */
  @Setter
  private String namespace;

  private Environment environment;

  private ApplicationContext applicationContext;

  @Setter
  protected String spELArgsName = "spELArgs";

  private final Map<String, StateCenter> stateCenterMap = new ConcurrentHashMap<>();

  public Object invokeWithinIdempotence(IdempotenceAttr attr, Object[] arguments,
                                        TSupplier<Object> supplier, Method method) throws Throwable {

    String spEL = attr.getSpEL();
    String rawKey = SpELUtils.getKey(spEL, method, spELArgsName, arguments);

    if (rawKey == null || rawKey.isEmpty()) {
      String msg = String.format("`rawKey` of idempotence operation can not be null or \"\". arguments: [%s], spEL: [%s].",
              JacksonUtils.toStr(arguments), spEL);
      throw new IdempotenceException(msg, ConsumeStage.ARG_CHECK);
    }

    IdempotenceParamWrapper idempotenceParamWrapper = new IdempotenceParamWrapper();
    idempotenceParamWrapper.setSpEL(attr.getSpEL());
    idempotenceParamWrapper.setRepeatConsumptionMsg(attr.getRepeatConsumptionMsg());
    idempotenceParamWrapper.setWaitTimeoutMsg(attr.getWaitTimeoutMsg());
    idempotenceParamWrapper.setScenario(attr.getScenario());
    idempotenceParamWrapper.setStateCenter(attr.getStateCenter());
    idempotenceParamWrapper.setConsumeMode(attr.getConsumeMode());
    idempotenceParamWrapper.setTimeUnit(attr.getTimeUnit());
    idempotenceParamWrapper.setExpectCost(attr.getExpectCost());
    idempotenceParamWrapper.setTtl(attr.getTtl());
    // 补充非用户层传来的字段
    idempotenceParamWrapper.setNamespace(namespace);
    idempotenceParamWrapper.setRawKey(rawKey);
    idempotenceParamWrapper.setXId(UUID.randomUUID().toString().replace("-", ""));
    idempotenceParamWrapper.setTtlInSecs((int) attr.getTimeUnit().toSeconds(attr.getTtl()));

    switch (attr.getConsumeMode()) {
      case SUCCESS:
        return handleSuccessMode(idempotenceParamWrapper, supplier, true, arguments);
      case COMPLETE:
        return handleCompleteMode(idempotenceParamWrapper, supplier, arguments);
      default: // unreachable
        throw SystemException.unExpectedException();
    }
  }

  /**
   * 对于新数据：<br/>
   * 开始消费之前，将数据置为 {@link ConsumeState#CONSUMING}。 <br/>
   * 消费正常完成，将数据置为 {@link ConsumeState#SUCCESS}。 <br/>
   * 消费异常完成，将数据置为 {@link ConsumeState#EXCEPTION}。
   * <p/>
   * 对于重复消费数据：<br/>
   * 如果上一条数据是正在消费中，等待后重试。 <br/>
   * 如果上一条数据是消费异常完成，本次按新数据消费。 <br/>
   * 如果上一条数据是消费正常完成，本次消费进入重复消费逻辑。
   */
  private Object handleSuccessMode(IdempotenceParamWrapper param, TSupplier<Object> supplier,
                                   boolean retry, Object[] arguments) throws Throwable {
    return handle(param, arguments, new TBiFunction<StateCenter, ConsumeState, Object>() {
      @Override
      public Object apply(StateCenter stateCenter, ConsumeState consumeState) throws Throwable {
        if (consumeState == null) {
          return consume(stateCenter, param, supplier, arguments);
        } else if (consumeState == ConsumeState.EXCEPTION) {
          return reConsumeAfterException(stateCenter, param, supplier, arguments);
        } else if (consumeState == ConsumeState.SUCCESS) {
          return repeatConsume(stateCenter, param, arguments);
        } else if (consumeState == ConsumeState.CONSUMING) {
          return concurrentConsume(stateCenter, param, supplier, retry, arguments);
        }
        return null; // unreachable
      }
    });
  }

  private Object concurrentConsume(StateCenter stateCenter, IdempotenceParamWrapper param,
                                   TSupplier<Object> supplier, boolean retry, Object[] arguments) throws Throwable {
    if (retry) {
      SleepUtils.sleep(param.getTimeUnit(), param.getExpectCost());
      return handleSuccessMode(param, supplier, false, arguments);
    } else {
      IdempotenceScenario scenario = param.getScenario();
      switch (scenario) {
        case MQ:
          String msg1 = String.format("[%s] has always been in consumption. Expected cost: [%s ms].", param.getKey(), param.getTimeUnit().toMillis(param.getExpectCost()));
          throw throwableCallback(stateCenter, param, arguments, ConsumeStage.WAIT_TIMEOUT, msg1, new IdempotenceWaitTimeoutException(msg1));
        case REQUEST:
          String waitTimeoutMsg = param.getWaitTimeoutMsg();
          String msg2 = ValueResolverHelper.resolveDollarPlaceholder(waitTimeoutMsg, assembleProperties(param));
          throw new ComponentServiceException(msg2);
        default: // unreachable
          throw SystemException.unExpectedException();
      }
    }
  }

  private Object reConsumeAfterException(StateCenter stateCenter, IdempotenceParamWrapper param,
                                         TSupplier<Object> supplier, Object[] arguments) throws Throwable {
    boolean b;
    try {
      b = stateCenter.casState(param, ConsumeState.EXCEPTION, ConsumeState.CONSUMING, true);
    } catch (Throwable e) {
      Throwable throwable = throwableCallback(stateCenter, param, arguments, ConsumeStage.RETRY_ERROR,
              "CAS EXCEPTION to CONSUMING error.", e);
      try {
        RetryUtils.retryWhenException(() -> stateCenter.casState(param, ConsumeState.CONSUMING, ConsumeState.EXCEPTION, false), param);
      } catch (Throwable e1) {
        throw throwableCallback(stateCenter, param, arguments, ConsumeStage.RETRY_ERROR_AND_RESET_ERROR, "Reset CONSUMING to EXCEPTION error.", e1);
      }
      throw throwable;
    }
    if (b) {
      return consume(stateCenter, param, supplier, arguments);
    } else {
      SleepUtils.sleep(param.getTimeUnit(), param.getExpectCost());
      return handleSuccessMode(param, supplier, true, arguments);
    }
  }

  /**
   * 数据当前状态无论是 {@link ConsumeState#CONSUMING}/{@link ConsumeState#SUCCESS}/{@link ConsumeState#EXCEPTION}，都认为消费过。
   */
  private Object handleCompleteMode(IdempotenceParamWrapper param, TSupplier<Object> supplier,
                                    Object[] arguments) throws Throwable {
    return handle(param, arguments, new TBiFunction<StateCenter, ConsumeState, Object>() {
      @Override
      public Object apply(StateCenter stateCenter, ConsumeState consumeState) throws Throwable {
        if (consumeState == null) {
          return consume(stateCenter, param, supplier, arguments);
        } else {
          return repeatConsume(stateCenter, param, arguments);
        }
      }
    });
  }

  private Object handle(IdempotenceParamWrapper param, Object[] arguments,
                        TBiFunction<StateCenter, ConsumeState, Object> function) throws Throwable {
    String stateCenterName = param.getStateCenter();
    StateCenter stateCenter = stateCenterMap.computeIfAbsent(stateCenterName, s -> (StateCenter) applicationContext.getBean(stateCenterName));
    ConsumeState consumeState = setConsumingStateIfAbsent(param, stateCenter, arguments);
    return function.apply(stateCenter, consumeState);
  }

  private ConsumeState setConsumingStateIfAbsent(IdempotenceParamWrapper param,
                                                 StateCenter stateCenter, Object[] arguments) throws Throwable {
    try {
      return stateCenter.getSetIfAbsent(param, ConsumeState.CONSUMING);
    } catch (Throwable e) {
      Throwable bizThrowable = throwableCallback(stateCenter, param, arguments, ConsumeStage.BEFORE_CONSUMPTION,
              "GetSetIfAbsent CONSUMING state error.", e);
      try {
        // 这里出现异常的概率非常高，因为getSetIfAbsent()失败，表示当前容器和状态中心的通信异常。
        RetryUtils.retryWhenException(() -> stateCenter.delete(param), param);
      } catch (Throwable throwable) {
        throw throwableCallback(stateCenter, param, arguments, ConsumeStage.GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR,
                "GetSetIfAbsent CONSUMING state error and delete key error.", throwable);
      }
      throw bizThrowable;
    }
  }

  private Object consume(StateCenter stateCenter, IdempotenceParamWrapper param,
                         TSupplier<Object> supplier, Object[] arguments) throws Throwable {
    Object o;
    try {
      o = supplier.get();
    } catch (ServiceException e) {
      // MQ场景下，如果发生ServiceException，和其他异常一样处理
      // REQUEST场景下，如果发生了ServiceException，不能阻塞用户的重试
      IdempotenceScenario scenario = param.getScenario();
      switch (scenario) {
        case REQUEST:
          try {
            RetryUtils.retryWhenException(() -> stateCenter.delete(param), param);
          } catch (Throwable e1) {
            throw throwableCallback(stateCenter, param, arguments, ConsumeStage.SERVICE_EXCEPTION_AND_DELETE_ERROR,
                    "ServiceException and delete error.", e1);
          }
          throw e;
        case MQ:
          throw casConsumingToException(stateCenter, param, arguments, e);
        default: // unreachable
          throw SystemException.unExpectedException();
      }
    } catch (Throwable e) {
      throw casConsumingToException(stateCenter, param, arguments, e);
    }
    try {
      RetryUtils.retryWhenException(() -> stateCenter.casState(param, ConsumeState.CONSUMING, ConsumeState.SUCCESS, false), param);
    } catch (Throwable e) {
      throwableCallback(stateCenter, param, arguments, ConsumeStage.AFTER_CONSUMPTION,
              "CAS CONSUMING to SUCCESS error.", e);
    }
    return o;
  }

  private Throwable casConsumingToException(StateCenter stateCenter, IdempotenceParamWrapper param,
                                            Object[] arguments, Throwable bizThrowable) throws Throwable {
    // 业务异常的throwable。是要扔出去的异常。
    Throwable throwable = throwableCallback(stateCenter, param, arguments, ConsumeStage.IN_CONSUMPTION, "Biz consumption error.", bizThrowable);
    try {
      RetryUtils.retryWhenException(() -> stateCenter.casState(param, ConsumeState.CONSUMING, ConsumeState.EXCEPTION, false), param);
    } catch (Throwable t) {
      // 如果CAS CONSUMING到EXCEPTION失败。
      throw throwableCallback(stateCenter, param, arguments, ConsumeStage.CAS_CONSUMING_TO_EXCEPTION_ERROR, "CAS CONSUMING to EXCEPTION error.", t);
    }
    return throwable;
  }

  /**
   * MQ：打印error日志。 <br/>
   * REQUEST：抛ServiceException，通知用户。
   */
  private Object repeatConsume(StateCenter stateCenter, IdempotenceParamWrapper param, Object[] arguments) throws Throwable {
    IdempotenceScenario scenario = param.getScenario();
    switch (scenario) {
      case MQ:
        String msg1 = String.format("[%s] has consumed.", param.getKey());
        throw throwableCallback(stateCenter, param, arguments, ConsumeStage.REPEATED_CONSUMPTION, msg1, new IdempotenceRepeatedConsumptionException(msg1));
      case REQUEST:
        String repeatConsumptionMsg = param.getRepeatConsumptionMsg();
        String msg2 = ValueResolverHelper.resolveDollarPlaceholder(repeatConsumptionMsg, assembleProperties(param));
        throw new ComponentServiceException(msg2);
      default: // unreachable
        throw SystemException.unExpectedException();
    }
  }

  private Properties assembleProperties(IdempotenceParamWrapper param) {
    Properties properties = new Properties();
    properties.put("prefix", param.getPrefix());
    if (StringUtils.hasLength(param.getSpEL())) {
      properties.put("spEL", param.getSpEL());
    }
    properties.put("repeatConsumptionMsg", param.getRepeatConsumptionMsg());
    properties.put("waitTimeoutMsg", param.getWaitTimeoutMsg());
    properties.put("namespace", param.getNamespace());
    properties.put("xId", param.getXId());
    properties.put("prefix", param.getPrefix());
    properties.put("ttlInSecs", param.getTtlInSecs());
    properties.put("scenario", param.getScenario());
    properties.put("stateCenter", param.getStateCenter());
    properties.put("consumeMode", param.getConsumeMode());
    properties.put("timeUnit", param.getTimeUnit());
    properties.put("expectCost", param.getExpectCost());
    properties.put("expectCostStr", param.getExpectCost() + TimeUnitUtils.aliasTimeUnit(param.getTimeUnit()));
    properties.put("ttl", param.getTtl());
    properties.put("ttlStr", param.getTtl() + TimeUnitUtils.aliasTimeUnit(param.getTimeUnit()));
    properties.put("rawKey", param.getRawKey());
    properties.put("simpleKey", param.getSimpleKey());
    properties.put("key", param.getKey());
    return properties;
  }

  private Throwable throwableCallback(StateCenter stateCenter, IdempotenceParamWrapper param,
                                      Object[] arguments, ConsumeStage consumeStage, String info, Throwable throwable) {
    String msg = String.format("Exception occurred in [%s] stage. %s", consumeStage, info);
    stateCenter.saveExceptionLog(param, arguments, consumeStage, msg, throwable);
    if (consumeStage == ConsumeStage.IN_CONSUMPTION) {
      return throwable;
    }
    return new IdempotenceException(msg, consumeStage, param.getXId(), throwable);
  }

  private String resolve(String value) {
    if (StringUtils.hasText(value)) {
      return this.environment.resolvePlaceholders(value);
    }
    return value;
  }

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    this.namespace = resolve(namespace);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

}
