package cn.addenda.component.idempotence;

import lombok.*;

/**
 * @author addenda
 * @since 2023/7/29 18:07
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class IdempotenceParamWrapper extends IdempotenceAttr {

  private String namespace;

  private String rawKey;

  private String xId;

  /**
   * 此参数是由{@link IdempotenceAttr#getTimeUnit()}和@{@link IdempotenceAttr#getTtl()}构成。
   * 单独设立一个参数为了后续便于使用
   */
  private int ttlInSecs;

  public IdempotenceKey toKey() {
    return IdempotenceKey.of(namespace, getPrefix(), rawKey);
  }

  public String getKey() {
    return toKey().getKey();
  }

  public String getSimpleKey() {
    return toKey().getSimpleKey();
  }

}
