package cn.addenda.component.idempotence.test;

import cn.addenda.component.idempotence.EnableIdempotenceManagement;
import cn.addenda.component.idempotence.IdempotenceHelper;
import cn.addenda.component.idempotence.statecenter.DbStateCenter;
import cn.addenda.component.idempotence.statecenter.RedisStateCenter;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringValueResolver;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author addenda
 * @since 2023/7/31 15:28
 */
@EnableIdempotenceManagement
@Configuration
public class IdempotenceTestConfiguration implements EmbeddedValueResolverAware {

  static Properties dbProperties;
  static Properties redisProperties;

  static {
    try {
      String path = IdempotenceTestConfiguration.class.getClassLoader()
              .getResource("db.properties").getPath();
      dbProperties = new Properties();
      dbProperties.load(new FileInputStream(path));
    } catch (Exception e) {

    }

    try {
      String path = IdempotenceTestConfiguration.class.getClassLoader()
              .getResource("redis.properties").getPath();
      redisProperties = new Properties();
      redisProperties.load(new FileInputStream(path));
    } catch (Exception e) {

    }
  }

  private StringValueResolver resolver;

  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
    redisStandaloneConfiguration.setHostName(redisProperties.getProperty("host"));
    redisStandaloneConfiguration.setPort(Integer.parseInt(redisProperties.getProperty("port")));
    redisStandaloneConfiguration.setPassword(redisProperties.getProperty("password"));
    return new LettuceConnectionFactory(redisStandaloneConfiguration);
  }

  @Bean
  public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
    return new StringRedisTemplate(redisConnectionFactory);
  }

  @Bean
  public RedisStateCenter redisStateCenter(StringRedisTemplate stringRedisTemplate) {
    return new RedisStateCenter(stringRedisTemplate);
  }

  @Bean
  public DataSource dataSource() {
    HikariDataSource hikariDataSource = new HikariDataSource();
    hikariDataSource.setDriverClassName(dbProperties.getProperty("driver"));
    hikariDataSource.setJdbcUrl(dbProperties.getProperty("url"));
    hikariDataSource.setUsername(dbProperties.getProperty("username"));
    hikariDataSource.setPassword(dbProperties.getProperty("password"));
    hikariDataSource.setMaximumPoolSize(3);

    return hikariDataSource;
  }

  @Bean
  public DbStateCenter dbStateCenter(DataSource dataSource) {
    return new DbStateCenter(dataSource);
  }

  @Bean
  public IdempotenceHelper idempotenceHelper() {
    IdempotenceHelper idempotenceHelper = new IdempotenceHelper();
    idempotenceHelper.setNamespace("idempotence");
    return idempotenceHelper;
  }

  @Override
  public void setEmbeddedValueResolver(StringValueResolver resolver) {
    this.resolver = resolver;
  }
}
