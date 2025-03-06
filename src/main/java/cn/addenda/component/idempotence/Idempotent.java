package cn.addenda.component.idempotence;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/7/28 17:26
 */
@Inherited
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Idempotent {

  /**
   * {@link IdempotenceAttr#getPrefix()}
   */
  String prefix() default IdempotenceAttr.DEFAULT_PREFIX;

  /**
   * {@link IdempotenceAttr#getSpEL()}
   */
  String spEL() default "";

  /**
   * {@link IdempotenceAttr#getRepeatConsumptionMsg()}
   */
  String repeatConsumptionMsg() default IdempotenceAttr.DEFAULT_REPEAT_CONSUMPTION_MSG;

  /**
   * {@link IdempotenceAttr#getWaitTimeoutMsg()}
   */
  String waitTimeoutMsg() default IdempotenceAttr.DEFAULT_WAIT_TIMEOUT_MSG;

  /**
   * {@link IdempotenceAttr#getScenario()}
   */
  IdempotenceScenario scenario();

  /**
   * {@link IdempotenceAttr#getStateCenter()}。状态中心的bean名称。
   */
  String stateCenter() default IdempotenceAttr.DEFAULT_STATE_CENTER;

  /**
   * {@link IdempotenceAttr#getConsumeMode()}
   */
  ConsumeMode consumeMode() default ConsumeMode.SUCCESS;

  /**
   * {@link IdempotenceAttr#getTimeUnit()}
   */
  TimeUnit timeUnit() default TimeUnit.SECONDS;

  /**
   * todo expectCost动态计算
   * {@link IdempotenceAttr#getExpectCost()}
   */
  int expectCost() default IdempotenceAttr.DEFAULT_EXPECT_COST;

  /**
   * {@link IdempotenceAttr#getTtl()}
   */
  int ttl() default IdempotenceAttr.DEFAULT_TTL;

}
