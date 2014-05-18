package eu.codesigner.redis;

import static redis.netty4.BulkReply.NIL_REPLY;
import static redis.netty4.StatusReply.OK;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;
import redis.netty4.BulkReply;
import redis.netty4.IntegerReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.Reply;
import redis.netty4.StatusReply;
import redis.server.netty.RedisException;
import redis.server.netty.RedisServer;
import redis.server.netty.SimpleRedisServer;

public class JedisProxyRedisServer implements ExtendedRedisServer {
  private static final RedisServer srs = new SimpleRedisServer();

  private long getLong(byte[] ba) {
    return Long.parseLong((new String(ba)));
  }

  private int getInt(byte[] ba) {
    return Integer.parseInt((new String(ba)));
  }

  enum Type {
    DATA, MONITORING
  }

  private static Map<Type, JedisPool> pools = new HashMap<>();

  @FunctionalInterface
  private interface RedisRunner {
    public Object doRun(Jedis j) throws RedisException;
  }

  JedisProxyRedisServer() {
    JedisPoolConfig jpc = new JedisPoolConfig();
    jpc.setMinIdle(ProxyConf.jMinIdle);
    jpc.setMaxIdle(ProxyConf.jMaxIdle);
    jpc.setMaxTotal(ProxyConf.jMaxIdle);

    JedisPool pool1 = new JedisPool(jpc, ProxyConf.redisHost, ProxyConf.redisPort, ProxyConf.jconnectTimeout);
    pools.put(Type.DATA, pool1);
    JedisPool pool2 = new JedisPool(jpc, ProxyConf.redisHost, ProxyConf.redisPort, ProxyConf.jconnectTimeout);
    pools.put(Type.MONITORING, pool2);
  }

  public IntegerReply append(byte[] key0, byte[] value1) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public IntegerReply bitcount(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
    return null;
  }

  public IntegerReply bitop(byte[] operation0, byte[] destkey1, byte[][] key2) throws RedisException {
    return null;
  }

  public IntegerReply decr(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply decrby(byte[] key0, byte[] decrement1) throws RedisException {
    return null;
  }

  private Object withJedis(Type t, RedisRunner r) {
    JedisPool jp = pools.get(t);
    Jedis j = jp.getResource();
    try {
      Object res = r.doRun(j);
      jp.returnResourceObject(j);
      return res;
    }
    catch (Throwable th) {
      jp.returnBrokenResource(j);
      th.printStackTrace();
      // Can't return error reply because it can't cast to multibulkreply
      return null;
    }

  }

  public BulkReply get(byte[] key0) throws RedisException {
    return (BulkReply) withJedis(Type.DATA, (j) -> {
      byte[] res = j.get(key0);
      return res == null ? NIL_REPLY : new BulkReply(res);
    });
  }

  public IntegerReply getbit(byte[] key0, byte[] offset1) throws RedisException {
    return null;
  }

  public BulkReply getrange(byte[] key0, byte[] start1, byte[] end2) throws RedisException {
    return null;
  }

  public BulkReply getset(byte[] key0, byte[] value1) throws RedisException {
    return null;
  }

  public IntegerReply incr(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply incrby(byte[] key0, byte[] increment1) throws RedisException {
    return null;
  }

  public BulkReply incrbyfloat(byte[] key0, byte[] increment1) throws RedisException {
    return null;
  }

  public MultiBulkReply mget(byte[][] key0) throws RedisException {
    return null;
  }

  public StatusReply mset(byte[][] key_or_value0) throws RedisException {
    return null;
  }

  public IntegerReply msetnx(byte[][] key_or_value0) throws RedisException {
    return null;
  }

  public Reply psetex(byte[] key0, byte[] milliseconds1, byte[] value2) throws RedisException {
    return null;
  }

  public StatusReply set(byte[] key0, byte[] value1) throws RedisException {
    if (value1 == null)
      return null;
    return (StatusReply) withJedis(Type.DATA, (j) -> {
      return j.set(key0, value1) == null ? NIL_REPLY : OK;
    });
  }

  public IntegerReply setbit(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
    return null;
  }

  public StatusReply setex(byte[] key0, byte[] seconds1, byte[] value2) throws RedisException {
    return null;
  }

  public IntegerReply setnx(byte[] key0, byte[] value1) throws RedisException {
    return null;
  }

  public IntegerReply setrange(byte[] key0, byte[] offset1, byte[] value2) throws RedisException {
    return null;
  }

  public IntegerReply strlen(byte[] key0) throws RedisException {
    return null;
  }

  public BulkReply echo(byte[] message0) throws RedisException {
    return null;
  }

  public StatusReply ping() throws RedisException {
    return srs.ping();
  }

  public StatusReply quit() throws RedisException {
    return srs.quit();
  }

  public StatusReply select(byte[] index0) throws RedisException {
    return null;
  }

  public StatusReply bgrewriteaof() throws RedisException {
    return null;
  }

  public StatusReply bgsave() throws RedisException {
    return null;
  }

  public Reply client_kill(byte[] ip_port0) throws RedisException {
    return null;
  }

  public Reply client_list() throws RedisException {
    return null;
  }

  public Reply client_getname() throws RedisException {
    return null;
  }

  public Reply client_setname(byte[] connection_name0) throws RedisException {
    return null;
  }

  public Reply config_get(byte[] parameter0) throws RedisException {
    return null;
  }

  public Reply config_set(byte[] parameter0, byte[] value1) throws RedisException {
    return null;
  }

  public Reply config_resetstat() throws RedisException {
    return null;
  }

  public IntegerReply dbsize() throws RedisException {
    return null;
  }

  public Reply debug_object(byte[] key0) throws RedisException {
    return null;
  }

  public Reply debug_segfault() throws RedisException {
    return null;
  }

  public StatusReply flushall() throws RedisException {
    return null;
  }

  public StatusReply flushdb() throws RedisException {
    return null;
  }

  public BulkReply info(byte[] section0) throws RedisException {
    // TODO Auto-generated method stub
    return srs.info(section0);
  }

  public IntegerReply lastsave() throws RedisException {
    return null;
  }

  public Reply monitor() throws RedisException {
    return null;
  }

  public Reply save() throws RedisException {
    return null;
  }

  public StatusReply shutdown(byte[] NOSAVE0, byte[] SAVE1) throws RedisException {
    return null;
  }

  public StatusReply slaveof(byte[] host0, byte[] port1) throws RedisException {
    return null;
  }

  public Reply slowlog(byte[] subcommand0, byte[] argument1) throws RedisException {
    return null;
  }

  public Reply sync() throws RedisException {
    return null;
  }

  public MultiBulkReply time() throws RedisException {
    return srs.time();
  }

  private MultiBulkReply bXpop(boolean isLeft, byte[][] key0) throws RedisException {
    return (MultiBulkReply) withJedis(Type.DATA, (j) -> {
      List<byte[]> ret;
      if (isLeft) {
        ret = j.blpop(key0);
      }
      else {
        ret = j.brpop(key0);
      }
      // TODO: if ret is null, the list does not exist and we should have returned NIL_REPLY, but cast clash happens.
        if (ret == null || ret.size() == 0)
          return MultiBulkReply.EMPTY;
        return collectionToMultiBulkReply(ret);
      });

  }

  public MultiBulkReply blpop(byte[][] key0) throws RedisException {
    return bXpop(true, key0);
  }

  public MultiBulkReply brpop(byte[][] key0) throws RedisException {
    return bXpop(false, key0);
  }

  public BulkReply brpoplpush(byte[] source0, byte[] destination1, byte[] timeout2) throws RedisException {
    return null;
  }

  public BulkReply lindex(byte[] key0, byte[] index1) throws RedisException {
    return null;
  }

  public IntegerReply linsert(byte[] key0, byte[] where1, byte[] pivot2, byte[] value3) throws RedisException {
    return null;
  }

  public IntegerReply llen(byte[] key0) throws RedisException {
    return null;
  }

  public BulkReply lpop(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply lpush(byte[] key0, byte[][] value1) throws RedisException {
    return (IntegerReply) withJedis(Type.DATA, (j) -> {
      Long res = j.lpush(key0, value1);
      return new IntegerReply(res);
    });
  }

  public IntegerReply lpushx(byte[] key0, byte[] value1) throws RedisException {
    return null;
  }

  public MultiBulkReply lrange(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    return null;
  }

  public IntegerReply lrem(byte[] key0, byte[] count1, byte[] value2) throws RedisException {
    return null;
  }

  public StatusReply lset(byte[] key0, byte[] index1, byte[] value2) throws RedisException {
    return null;
  }

  public StatusReply ltrim(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    return null;
  }

  public BulkReply rpop(byte[] key0) throws RedisException {
    return null;
  }

  public BulkReply rpoplpush(byte[] source0, byte[] destination1) throws RedisException {
    return null;
  }

  public IntegerReply rpush(byte[] key0, byte[][] value1) throws RedisException {
    return (IntegerReply) withJedis(Type.DATA, (j) -> {
      Long res = j.rpush(key0, value1);
      return new IntegerReply(res);
    });
  }

  public IntegerReply rpushx(byte[] key0, byte[] value1) throws RedisException {
    return null;
  }

  public IntegerReply del(byte[][] key0) throws RedisException {
    return null;
  }

  public BulkReply dump(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply exists(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply expire(byte[] key0, byte[] seconds1) throws RedisException {
    return (IntegerReply) withJedis(Type.DATA, (j) -> {
      int secs = ByteBuffer.wrap(seconds1).getInt();
      Long res = j.expire(key0, secs);
      return new IntegerReply(res);
    });
  }

  public IntegerReply expireat(byte[] key0, byte[] timestamp1) throws RedisException {
    return null;
  }

  public MultiBulkReply keys(byte[] pattern0) throws RedisException {
    return null;
  }

  public StatusReply migrate(byte[] host0, byte[] port1, byte[] key2, byte[] destination_db3, byte[] timeout4) throws RedisException {
    return null;
  }

  public IntegerReply move(byte[] key0, byte[] db1) throws RedisException {
    return null;
  }

  public Reply object(byte[] subcommand0, byte[][] arguments1) throws RedisException {
    return null;
  }

  public IntegerReply persist(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply pexpire(byte[] key0, byte[] milliseconds1) throws RedisException {
    return null;
  }

  public IntegerReply pexpireat(byte[] key0, byte[] milliseconds_timestamp1) throws RedisException {
    return null;
  }

  public IntegerReply pttl(byte[] key0) throws RedisException {
    return null;
  }

  public BulkReply randomkey() throws RedisException {
    return null;
  }

  public StatusReply rename(byte[] key0, byte[] newkey1) throws RedisException {
    return null;
  }

  public IntegerReply renamenx(byte[] key0, byte[] newkey1) throws RedisException {
    return null;
  }

  public StatusReply restore(byte[] key0, byte[] ttl1, byte[] serialized_value2) throws RedisException {
    return null;
  }

  public Reply sort(byte[] key0, byte[][] pattern1) throws RedisException {
    return null;
  }

  public IntegerReply ttl(byte[] key0) throws RedisException {
    return null;
  }

  public StatusReply type(byte[] key0) throws RedisException {
    return null;
  }

  public StatusReply unwatch() throws RedisException {
    return null;
  }

  public StatusReply watch(byte[][] key0) throws RedisException {
    return null;
  }

  public Reply eval(byte[] script0, byte[] numkeys1, byte[][] key2) throws RedisException {
    return null;
  }

  public Reply evalsha(byte[] sha10, byte[] numkeys1, byte[][] key2) throws RedisException {
    return null;
  }

  public Reply script_exists(byte[][] script0) throws RedisException {
    return null;
  }

  public Reply script_flush() throws RedisException {
    return null;
  }

  public Reply script_kill() throws RedisException {
    return null;
  }

  public Reply script_load(byte[] script0) throws RedisException {
    return null;
  }

  public IntegerReply hdel(byte[] key0, byte[][] field1) throws RedisException {
    return null;
  }

  public IntegerReply hexists(byte[] key0, byte[] field1) throws RedisException {
    return null;
  }

  public BulkReply hget(byte[] key0, byte[] field1) throws RedisException {
    return null;
  }

  public MultiBulkReply hgetall(byte[] key0) throws RedisException {
    return (MultiBulkReply) withJedis(Type.DATA, (j) -> {
      Map<byte[], byte[]> res = j.hgetAll(key0);
      if (res == null) {
        return MultiBulkReply.EMPTY;
      }

      int size = res.size();
      if (size == 0) {
        return MultiBulkReply.EMPTY;
      }

      Reply[] replies = new Reply[res.size() * 2];
      int i = 0;
      for (byte[] b : res.keySet()) {
        replies[i++] = new BulkReply(b);
        replies[i++] = new BulkReply(res.get(b));
      }
      return new MultiBulkReply(replies);
    });
  }

  public IntegerReply hincrby(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
    return null;
  }

  public BulkReply hincrbyfloat(byte[] key0, byte[] field1, byte[] increment2) throws RedisException {
    return null;
  }

  public MultiBulkReply hkeys(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply hlen(byte[] key0) throws RedisException {
    return null;
  }

  public MultiBulkReply hmget(byte[] key0, byte[][] field1) throws RedisException {
    return null;
  }

  public StatusReply hmset(byte[] key0, byte[][] field_or_value1) throws RedisException {
    return (StatusReply) withJedis(Type.DATA, (j) -> {
      Map<byte[], byte[]> map = new HashMap<>();
      for (int i = 0; i < field_or_value1.length; i++) {
        map.put(field_or_value1[i++], field_or_value1[i++]);
      }
      return j.hmset(key0, map) == null ? NIL_REPLY : OK;
    });
  }

  public IntegerReply hset(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
    return null;
  }

  public IntegerReply hsetnx(byte[] key0, byte[] field1, byte[] value2) throws RedisException {
    return null;
  }

  public MultiBulkReply hvals(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply publish(byte[] channel0, byte[] message1) throws RedisException {
    return (IntegerReply) withJedis(Type.DATA, (j) -> {
      Long res = j.publish(channel0, message1);
      return new IntegerReply(res);
    });
  }

  public IntegerReply sadd(byte[] key0, byte[][] member1) throws RedisException {
    return null;
  }

  public IntegerReply scard(byte[] key0) throws RedisException {
    return null;
  }

  public MultiBulkReply sdiff(byte[][] key0) throws RedisException {
    return null;
  }

  public IntegerReply sdiffstore(byte[] destination0, byte[][] key1) throws RedisException {
    return null;
  }

  public MultiBulkReply sinter(byte[][] key0) throws RedisException {
    return null;
  }

  public IntegerReply sinterstore(byte[] destination0, byte[][] key1) throws RedisException {
    return null;
  }

  public IntegerReply sismember(byte[] key0, byte[] member1) throws RedisException {
    return null;
  }

  public MultiBulkReply smembers(byte[] key0) throws RedisException {
    return (MultiBulkReply) withJedis(Type.DATA, (j) -> {
      Set<byte[]> members = j.smembers(key0);
      return collectionToMultiBulkReply(members);
    });
  }

  private MultiBulkReply collectionToMultiBulkReply(Collection<byte[]> members) {
    if (members == null) {
      return MultiBulkReply.EMPTY;
    }
    int size = members.size();
    if (size == 0) {
      return MultiBulkReply.EMPTY;
    }
    Reply[] replies = new Reply[size];
    int i = 0;
    for (byte[] m : members) {
      replies[i++] = new BulkReply(m);
    }
    return new MultiBulkReply(replies);
  }

  public IntegerReply smove(byte[] source0, byte[] destination1, byte[] member2) throws RedisException {
    return null;
  }

  public BulkReply spop(byte[] key0) throws RedisException {
    return null;
  }

  public Reply srandmember(byte[] key0, byte[] count1) throws RedisException {
    return null;
  }

  public IntegerReply srem(byte[] key0, byte[][] member1) throws RedisException {
    return null;
  }

  public MultiBulkReply sunion(byte[][] key0) throws RedisException {
    return null;
  }

  public IntegerReply sunionstore(byte[] destination0, byte[][] key1) throws RedisException {
    return null;
  }

  public IntegerReply zadd(byte[][] args) throws RedisException {
    return null;
  }

  public IntegerReply zcard(byte[] key0) throws RedisException {
    return null;
  }

  public IntegerReply zcount(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
    return null;
  }

  public BulkReply zincrby(byte[] key0, byte[] increment1, byte[] member2) throws RedisException {
    return null;
  }

  public IntegerReply zinterstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
    return null;
  }

  public MultiBulkReply zrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
    return (MultiBulkReply) withJedis(Type.DATA, (j) -> {
      if (withscores3 != null && withscores3.length == 10 && (new String(withscores3)).toUpperCase().equals("WITHSCORES")) {
        Set<Tuple> zset = j.zrangeWithScores(key0, getLong(start1), getLong(stop2));
        return tupleCollectiontoMultiBulkReply(zset);
      }
      else if (withscores3 == null || withscores3.length == 0) {
        Set<byte[]> zset = j.zrange(key0, getLong(start1), getLong(stop2));
        return collectionToMultiBulkReply(zset);
      }
      else {
        System.out.println(withscores3.length);
        throw new RedisException("syntax error");
      }
    });
  }

  private Object tupleCollectiontoMultiBulkReply(Set<Tuple> members) {
    if (members == null) {
      return MultiBulkReply.EMPTY;
    }
    int size = members.size();
    if (size == 0) {
      return MultiBulkReply.EMPTY;
    }
    Reply[] replies = new Reply[size * 2];
    int i = 0;
    for (Tuple t : members) {
      replies[i++] = new BulkReply(t.getElement().getBytes());
      replies[i++] = new BulkReply(Double.toString(t.getScore()).getBytes());
    }
    return new MultiBulkReply(replies);

  }

  public MultiBulkReply zrangebyscore(byte[] key0, byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4) throws RedisException {
    return zXangebyscore(false, key0, min1, max2, withscores_offset_or_count4);
  }

  public MultiBulkReply zrevrangebyscore(byte[] key0, byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4) throws RedisException {
    return zXangebyscore(true, key0, min1, max2, withscores_offset_or_count4);
  }

  private MultiBulkReply zXangebyscore(boolean isRev, byte[] key0, byte[] min1, byte[] max2, byte[][] withscores_offset_or_count4) throws RedisException {

    return (MultiBulkReply) withJedis(Type.DATA, (j) -> {
      byte[] _offset = null;
      byte[] _count = null;
      Set<byte[]> zset = null;

      if (withscores_offset_or_count4 != null && withscores_offset_or_count4.length == 3 && Arrays.equals(withscores_offset_or_count4[0], "LIMIT".getBytes())) {
        _offset = withscores_offset_or_count4[1];
        _count = withscores_offset_or_count4[2];
        // More efficient way from byte[] -> String -> int ?
        if (isRev) {
          zset = j.zrevrangeByScore(key0, min1, max2, getInt(_offset), getInt(_count));
        }
        else {
          zset = j.zrangeByScore(key0, min1, max2, Integer.parseInt(new String(_offset)), Integer.parseInt(new String(_count)));
        }
      }
      else if (withscores_offset_or_count4 == null || withscores_offset_or_count4.length == 0) {
        if (isRev) {
          zset = j.zrevrangeByScore(key0, min1, max2);
        }
        else {
          zset = j.zrangeByScore(key0, min1, max2);
        }
      }
      else {
        throw new RedisException("syntax error");
      }

      return collectionToMultiBulkReply(zset);

    });
  }

  public Reply zrank(byte[] key0, byte[] member1) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public IntegerReply zrem(byte[] key0, byte[][] member1) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public IntegerReply zremrangebyrank(byte[] key0, byte[] start1, byte[] stop2) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public IntegerReply zremrangebyscore(byte[] key0, byte[] min1, byte[] max2) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public MultiBulkReply zrevrange(byte[] key0, byte[] start1, byte[] stop2, byte[] withscores3) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public Reply zrevrank(byte[] key0, byte[] member1) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public BulkReply zscore(byte[] key0, byte[] member1) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }

  public IntegerReply zunionstore(byte[] destination0, byte[] numkeys1, byte[][] key2) throws RedisException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public void punsubscribe(ChannelHandlerContext ctx, byte[][] patterns) {

    xUnsubscribe(true, ctx, patterns);
  }
  
  @Override
  public void unsubscribe(ChannelHandlerContext ctx, byte[][] patterns) {
    xUnsubscribe(false, ctx, patterns);
  }
  
  private void xUnsubscribe(boolean isPattern, ChannelHandlerContext ctx, byte[][] patterns){
    BinaryJedisPubSub j = ctx.attr(OffLoopRedisCommandHandler.PUBSUB_JEDIS).get();
    if(j != null){
      if(isPattern){
        j.punsubscribe(patterns);
      }
      else{
        j.unsubscribe(patterns);
      }
    }
  }
  @Override
  public void subscribe(ChannelHandlerContext ctx, byte[][] channels) {
    byte[] subscribe = "subscribe".getBytes();

    withJedis(Type.MONITORING, (j) -> {
      BinaryJedisPubSub bjps = new BinaryJedisPubSub() {

        @Override
        public void onUnsubscribe(byte[] channel, int subscribedChannels) {}

        @Override
        public void onSubscribe(byte[] channel, int subscribedChannels) {
          Reply[] arr = new Reply[] { new BulkReply(subscribe), new BulkReply(channel), new IntegerReply(1) }; // Fake
          ctx.writeAndFlush(new MultiBulkReply(arr));
        }

        @Override
        public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
        }

        @Override
        public void onPSubscribe(byte[] pattern, int subscribedChannels) {
        }

        @Override
        public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
        }

        @Override
        public void onMessage(byte[] channel, byte[] message) {
          ctx.writeAndFlush(new MultiBulkReply(new BulkReply[] { new BulkReply("message".getBytes()), new BulkReply(channel), new BulkReply(message) }));
        }
      };
      // Before blocking the thread, make the pubsub available for when we need to unsubscribe
      ctx.attr(OffLoopRedisCommandHandler.PUBSUB_JEDIS).set(bjps);
      j.subscribe(bjps, channels);
      return null;
    });
  }
}
