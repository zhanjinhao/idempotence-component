package cn.addenda.component.idempotence;

import cn.addenda.component.base.exception.SystemException;

/**
 * @author addenda
 * @since 2023/7/29 19:07
 */
public class IdempotenceHelperException extends SystemException {

  public IdempotenceHelperException() {
  }

  public IdempotenceHelperException(String message) {
    super(message);
  }

  public IdempotenceHelperException(String message, Throwable cause) {
    super(message, cause);
  }

  public IdempotenceHelperException(Throwable cause) {
    super(cause);
  }

  public IdempotenceHelperException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  @Override
  public String moduleName() {
    return "idempotence-helper";
  }

  @Override
  public String componentName() {
    return "idempotence";
  }

}
