package cn.addenda.component.idempotence;

/**
 * @author addenda
 * @since 2023/9/14 20:08
 */
public enum ConsumeStage {
  /**
   * 参数校验
   */
  ARG_CHECK,
  /**
   * 未进入消费
   */
  BEFORE_CONSUMPTION,
  /**
   * 消费中
   */
  IN_CONSUMPTION,
  /**
   * 业务消费失败后，将状态从消费中CAS到消费异常又异常
   */
  CAS_CONSUMING_TO_EXCEPTION_ERROR,
  /**
   * GetSetIfAbsent时出现异常，且删除时又异常
   */
  GETSET_IF_ABSENT_ERROR_AND_DELETE_ERROR,
  /**
   * 消费成功后设置SUCCESS
   */
  AFTER_CONSUMPTION,
  /**
   * REQUEST场景出现ServiceException后删除key失败
   */
  SERVICE_EXCEPTION_AND_DELETE_ERROR,
  /**
   * key的状态为SUCCESS后再次消费
   */
  REPEATED_CONSUMPTION,
  /**
   * key的状态为CONSUMING，当前线程等待超时
   */
  WAIT_TIMEOUT,
  /**
   * CAS EXCEPTION TO CONSUMING ERROR
   */
  RETRY_ERROR,
  /**
   * CAS EXCEPTION TO CONSUMING ERROR 且 RESET CONSUMING TO EXCEPTION ERROR
   */
  RETRY_ERROR_AND_RESET_ERROR,
}