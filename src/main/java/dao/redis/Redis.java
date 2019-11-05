package dao.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Redis {
    private String redisAddr;
    private int redisPort;
    private int maxTotal, maxIdle;
    private static JedisPool jedisPool;

    Redis() {
        this.redisAddr = "10.245.142.213";
        this.redisPort = 6380;
        this.maxTotal = 20;
        this.maxIdle = 15;
        setRedisPool();
    }

    Redis(String redisAddr, int redisPort) {
        this.redisAddr = redisAddr;
        this.redisPort = redisPort;
        this.maxTotal = 20;
        this.maxIdle = 15;
        setRedisPool();
    }

    Redis(String redisAddr, int redisPort, int maxTotal, int maxIdle) {
        this.redisAddr = redisAddr;
        this.redisPort = redisPort;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        setRedisPool();
    }

    private void setRedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxTotal);
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPool = new JedisPool(jedisPoolConfig, redisAddr, redisPort);
    }

    public Jedis getRedisInstanceFromPool (){
        return jedisPool.getResource();
    }

    public void insertRedis (String key, String value){
        getRedisInstanceFromPool().set(key, value);
    }

    public void insertRedis (int key, String value) {
        insertRedis(Integer.toString(key),value);
    }

    public void insertRedis (int key, int value) {
        insertRedis(Integer.toString(key), Integer.toString(value));
    }

    public void insertRedis (String key, int value) {
        insertRedis(key, Integer.toString(value));
    }

}
