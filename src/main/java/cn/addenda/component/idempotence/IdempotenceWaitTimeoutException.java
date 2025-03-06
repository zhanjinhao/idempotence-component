package cn.addenda.component.idempotence;

import cn.addenda.component.base.exception.SystemException;

/**
 * @author addenda
 * @since 2023/7/29 19:07
 */
public class IdempotenceWaitTimeoutException extends SystemException {

  public IdempotenceWaitTimeoutException() {
  }

  public IdempotenceWaitTimeoutException(String message) {
    super(message);
  }

  public IdempotenceWaitTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public IdempotenceWaitTimeoutException(Throwable cause) {
    super(cause);
  }

  public IdempotenceWaitTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  @Override
  public String moduleName() {
    return "idempotence-wait-timeout";
  }

  @Override
  public String componentName() {
    return "idempotence";
  }

}
