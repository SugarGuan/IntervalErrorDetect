package dao.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.Config;

import java.io.Serializable;

public class Redis implements Serializable {

    /**
     *  Redis 类
     *  提供Redis数据库对象的操作功能，包括插入String、List等
     *  维护一个Redis池
     */
    private String redisAddr;
    private int redisPort;
    private int maxTotal, maxIdle;
    private static JedisPool jedisPool;
    private static Jedis jedis;

    /**
     * Redis() 构造函数
     * 设置Redis的地址、最大连接数和最大活跃数
     */
    public Redis() {
        this.redisAddr = Config.getRedisIP();
        this.redisPort = Config.getRedisPort();
        this.maxTotal = Config.getRedisMaxTotal();
        this.maxIdle = Config.getRedisMaxIdle();
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

    /**
     * setRedisPool()
     * 根据构造函数的内容，设置redis资源池
     */
    private void setRedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxTotal);
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPool = new JedisPool(jedisPoolConfig, redisAddr, redisPort);
    }

    /**
     * getRedisInstanceFromPool()
     * 返回Redis资源池中的一个Redis资源。使用前应确保setRedisPool已经被调用并生成了Redis资源池
     * Redis资源池提供jedis资源，（该对象）可向其他类提供Redis操作能力，包括插入、查询等。
     * @return jedis对象
     */
    public Jedis getRedisInstanceFromPool (){
        return jedisPool.getResource();
    }

    /**
     * insertRedisString()
     * 向Redis插入String
     * @param key 键
     * @param value 值
     */
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

    /**
     * insertRedisList()
     * 向Redis插入List
     * @param list 键
     * @param value 值
     */
    public void insertRedisList(String list, String value) {
        if (jedis==null)
            jedis = getRedisInstanceFromPool();
//        System.out.println("-----------------------");
//        System.out.println("Insert into Redis");
//        System.out.println(list);
//        System.out.println(value);
        jedis.lpush(list,value);
//        System.out.println("-----------------------");

    }

    public void insertRedisList(String list, Long value) {
        getRedisInstanceFromPool().lpush(list, Long.toString(value));
    }

    public void insertRedisList(String list, int value) {
        getRedisInstanceFromPool().lpush(list, Integer.toString(value));
    }

}
