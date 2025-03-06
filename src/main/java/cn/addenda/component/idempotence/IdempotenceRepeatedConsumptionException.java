package cn.addenda.component.idempotence;

import cn.addenda.component.base.exception.SystemException;

/**
 * @author addenda
 * @since 2023/7/29 19:07
 */
public class IdempotenceRepeatedConsumptionException extends SystemException {

  public IdempotenceRepeatedConsumptionException() {
  }

  public IdempotenceRepeatedConsumptionException(String message) {
    super(message);
  }

  public IdempotenceRepeatedConsumptionException(String message, Throwable cause) {
    super(message, cause);
  }

  public IdempotenceRepeatedConsumptionException(Throwable cause) {
    super(cause);
  }

  public IdempotenceRepeatedConsumptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  @Override
  public String moduleName() {
    return "idempotence-repeated-consumption";
  }

  @Override
  public String componentName() {
    return "idempotence";
  }

}
