package cn.addenda.component.idempotence.statecenter;

import cn.addenda.component.idempotence.ConsumeStage;
import cn.addenda.component.idempotence.ConsumeState;
import cn.addenda.component.idempotence.IdempotenceException;
import cn.addenda.component.idempotence.IdempotenceParamWrapper;

/**
 * @author addenda
 * @since 2023/7/29 15:10
 */
public interface StateCenter {

  /**
   * key不存在的时候，设置key状态。
   *
   * @return 返回旧的key状态
   */
  ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState);

  /**
   * 状态cas到指定的状态。
   * casOther=true:  cas的时候不判断xId，但是cas之后key的xId为当前线程。会更新ttl。
   * casOther=false: cas的时候判断xId，但是cas之后key的xId为当前线程。不会更新ttl。
   *
   * @param casOther 是否能cas其他XId设置的数据。
   */
  boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther);

  /**
   * 记录异常日志，方便后续处理。此方法不能抛异常。
   */
  void saveExceptionLog(IdempotenceParamWrapper param, Object[] arguments, ConsumeStage consumeStage, String message, Throwable e);

  /**
   * 删除本次消费的记录（xId一致才能删除）。幂等。
   */
  boolean delete(IdempotenceParamWrapper param);

  /**
   * 异常的后置处理。执行完成之后，不再有阻塞调用的问题。
   */
  void handle(IdempotenceException idempotenceException);

}
