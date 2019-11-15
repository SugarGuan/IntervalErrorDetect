package dao.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

public class Redis implements Serializable {
    private String redisAddr;
    private int redisPort;
    private int maxTotal, maxIdle;
    private static JedisPool jedisPool;

    public Redis() {
        this.redisAddr = "10.245.142.213";
        this.redisPort = 6380;
        this.maxTotal = 20;
        this.maxIdle = 15;
        setRedisPool();
    }

    public Redis(String redisAddr, int redisPort) {
        this.redisAddr = redisAddr;
        this.redisPort = redisPort;
        this.maxTotal = 20;
        this.maxIdle = 15;
        setRedisPool();
    }

    public Redis(String redisAddr, int redisPort, int maxTotal, int maxIdle) {
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

    public void insertRedisString (String key, String value){
        getRedisInstanceFromPool().set(key, value);

    }

    public void insertRedisString (int key, String value) {
        insertRedisString(Integer.toString(key),value);
    }

    public void insertRedisString (int key, int value) {
        insertRedisString(Integer.toString(key), Integer.toString(value));
    }

    public void insertRedisString (String key, int value) {
        insertRedisString(key, Integer.toString(value));
    }

    public void insertRedisList(String list, String value) {
        getRedisInstanceFromPool().lpush(list,value);
    }

    public void insertRedisList(String list, Long value) {
        getRedisInstanceFromPool().lpush(list, Long.toString(value));
    }

    public void insertRedisList(String list, int value) {
        getRedisInstanceFromPool().lpush(list, Integer.toString(value));
    }

}
