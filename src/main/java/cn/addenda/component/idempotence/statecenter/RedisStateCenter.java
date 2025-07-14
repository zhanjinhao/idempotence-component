package cn.addenda.component.idempotence.statecenter;

import cn.addenda.component.base.collection.ArrayUtils;
import cn.addenda.component.base.jackson.util.JacksonUtils;
import cn.addenda.component.idempotence.ConsumeStage;
import cn.addenda.component.idempotence.ConsumeState;
import cn.addenda.component.idempotence.IdempotenceParamWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * @author addenda
 * @since 2023/7/29 15:18
 */
@Slf4j
public class RedisStateCenter implements StateCenter {

  private final StringRedisTemplate stringRedisTemplate;

  private static final DefaultRedisScript<String> GET_SET_SCRIPT = new DefaultRedisScript<>();
  private static final DefaultRedisScript<Boolean> DO_CAS_STATE_1_SCRIPT = new DefaultRedisScript<>();
  private static final DefaultRedisScript<Boolean> DO_CAS_STATE_2_SCRIPT = new DefaultRedisScript<>();
  private static final DefaultRedisScript<Boolean> DELETE_SCRIPT = new DefaultRedisScript<>();

  static {
    GET_SET_SCRIPT.setScriptText(
            "local key = KEYS[1];"
                    + "local xId = ARGV[1];"
                    + "local consumeMode = ARGV[2];"
                    + "local consumeState = ARGV[3];"
                    + "local scenario = ARGV[4];"
                    + "local ttlSecs = ARGV[5];"
                    + "if(redis.call('exists', key) == 0) then "
                    + "     redis.call('hset', key, 'xId', xId); "
                    + "     redis.call('hset', key, 'consumeMode', consumeMode); "
                    + "     redis.call('hset', key, 'consumeState', consumeState); "
                    + "     redis.call('hset', key, 'scenario', scenario); "
                    + "     redis.call('expire', key, ttlSecs); "
                    + "     return nil; "
                    + "else "
                    + "     return redis.call('hget', key, 'consumeState');"
                    + "end ");
    GET_SET_SCRIPT.setResultType(String.class);

    DO_CAS_STATE_1_SCRIPT.setScriptText(
            "local key = KEYS[1];"
                    + "local xId = ARGV[1];"
                    + "local expected = ARGV[2];"
                    + "local consumeState = ARGV[3];"
                    + "local ttlSecs = ARGV[4];"
                    + "if(redis.call('hget', key, 'xId')) ~= xId then "
                    + "    return false "
                    + "else "
                    + "    if(redis.call('hget', key, 'consumeState')) == expected then "
                    + "         redis.call('hset', key, 'xId', xId); "
                    + "         redis.call('hset', key, 'consumeState', consumeState); "
                    + "         redis.call('expire', key, ttlSecs); "
                    + "         return true "
                    + "     else "
                    + "         return false "
                    + "     end "
                    + "end ");
    DO_CAS_STATE_1_SCRIPT.setResultType(Boolean.class);

    DO_CAS_STATE_2_SCRIPT.setScriptText(
            "local key = KEYS[1];"
                    + "local xId = ARGV[1];"
                    + "local expected = ARGV[2];"
                    + "local consumeState = ARGV[3];"
                    + "local ttlSecs = ARGV[4];"
                    + "if(redis.call('hget', key, 'consumeState')) == expected then "
                    + "    redis.call('hset', key, 'xId', xId); "
                    + "    redis.call('hset', key, 'consumeState', consumeState); "
                    + "    redis.call('expire', key, ttlSecs); "
                    + "    return true "
                    + "else "
                    + "    return false "
                    + "end");
    DO_CAS_STATE_2_SCRIPT.setResultType(Boolean.class);

    DELETE_SCRIPT.setScriptText(
            "local key = KEYS[1];"
                    + "local xId = ARGV[1];"
                    + "if(redis.call('hget', key, 'xId')) == xId then "
                    + "    redis.call('del', key);"
                    + "    return true "
                    + "else "
                    + "    return false "
                    + "end");
    DELETE_SCRIPT.setResultType(Boolean.class);
  }

  public RedisStateCenter(StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
  }

  @Override
  public ConsumeState getSetIfAbsent(IdempotenceParamWrapper param, ConsumeState consumeState) {
    String old = stringRedisTemplate.execute(GET_SET_SCRIPT, ArrayUtils.asArrayList(param.getKey())
            , param.getXId(), param.getConsumeMode().name(), consumeState.name(), String.valueOf(param.getScenario()), String.valueOf(param.getTtlInSecs()));
    if (old == null) {
      return null;
    }
    return ConsumeState.valueOf(old);
  }

  @Override
  public boolean casState(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState, boolean casOther) {
    return casOther ? doCasState2(param, expected, consumeState) : doCasState1(param, expected, consumeState);
  }

  private boolean doCasState1(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState) {
    return Boolean.TRUE.equals(stringRedisTemplate.execute(DO_CAS_STATE_1_SCRIPT, ArrayUtils.asArrayList(param.getKey()),
            param.getXId(), expected.name(), consumeState.name(), String.valueOf(param.getTtlInSecs())));
  }

  private boolean doCasState2(IdempotenceParamWrapper param, ConsumeState expected, ConsumeState consumeState) {
    return Boolean.TRUE.equals(stringRedisTemplate.execute(DO_CAS_STATE_2_SCRIPT, ArrayUtils.asArrayList(param.getKey()),
            param.getXId(), expected.name(), consumeState.name(), String.valueOf(param.getTtlInSecs())));
  }

  /**
   * 打印error日志。参数不存Redis，因为Redis数据在内存中，存Redis可能把内存打满进而影响到其他业务。 <br/>
   */
  @Override
  public void saveExceptionLog(IdempotenceParamWrapper param, Object[] arguments, ConsumeStage consumeStage, String message, Throwable throwable) {
    String argsJson = JacksonUtils.toStr(arguments);
    log.error("Consume [{}] error. Scenario: [{}], ConsumeMode: [{}]. ConsumeStage: [{}]. Message: [{}]. Arguments: [{}]. XId: [{}]",
            param.getKey(), param.getScenario(), param.getConsumeMode(), consumeStage, message, argsJson, param.getXId(), throwable);
  }

  @Override
  public boolean delete(IdempotenceParamWrapper param) {
    return Boolean.TRUE.equals(stringRedisTemplate.execute(
            DELETE_SCRIPT, ArrayUtils.asArrayList(param.getKey()), param.getXId()));
  }

}
