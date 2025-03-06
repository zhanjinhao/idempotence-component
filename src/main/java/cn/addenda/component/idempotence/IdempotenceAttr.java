package cn.addenda.component.idempotence;

import lombok.*;

import java.util.concurrent.TimeUnit;

/**
 * @author addenda
 * @since 2023/7/29 14:03
 */
@Setter
@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IdempotenceAttr {

  public static final String DEFAULT_PREFIX = "prefix";

  public static final String DEFAULT_REPEAT_CONSUMPTION_MSG = "数据 [${key}] 已处理过！";
  public static final String DEFAULT_WAIT_TIMEOUT_MSG = "数据 [${key}] 正在消费，请稍后再试！";

  public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

  public static final String DEFAULT_STATE_CENTER = "dbStateCenter";

  public static final int DEFAULT_TTL = 60 * 60 * 24;
  public static final int DEFAULT_EXPECT_COST = 1;

  @Builder.Default
  private String prefix = DEFAULT_PREFIX;

  private String spEL;

  @Builder.Default
  private String repeatConsumptionMsg = DEFAULT_REPEAT_CONSUMPTION_MSG;

  @Builder.Default
  private String waitTimeoutMsg = DEFAULT_WAIT_TIMEOUT_MSG;

  private IdempotenceScenario scenario;

  @Builder.Default
  private String stateCenter = DEFAULT_STATE_CENTER;

  @Builder.Default
  private ConsumeMode consumeMode = ConsumeMode.SUCCESS;

  @Builder.Default
  private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

  @Builder.Default
  private int expectCost = DEFAULT_EXPECT_COST;

  @Builder.Default
  private int ttl = DEFAULT_TTL;

}
