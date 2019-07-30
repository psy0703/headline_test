package com.dgmall.sparktest.dgmallTestV2.common

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @Author: Cedaris
  * @Date: 2019/7/23 14:38
  */
object RedisUtil {
  private val host: String = "127.0.0.1"
  private val port: Int = 6379

  //创建redis连接池
  private val jedisPoolConfig = new JedisPoolConfig()
  //最大连接数
  jedisPoolConfig.setMaxTotal(100)
  //最大空闲
  jedisPoolConfig.setMaxIdle(20)
  //最小空闲
  jedisPoolConfig.setMinIdle(20)
  //忙碌时是否等待
  jedisPoolConfig.setBlockWhenExhausted(true)
  //忙碌时的等待时长 毫秒
  jedisPoolConfig.setMaxWaitMillis(200000)
  //每次获得连接是否进行测试
  jedisPoolConfig.setTestOnBorrow(true)

  private val jedisPool = new JedisPool(jedisPoolConfig, host, port)
  //直接得到一个redis的连接
  val getJedisClient: Jedis = jedisPool.getResource
}
