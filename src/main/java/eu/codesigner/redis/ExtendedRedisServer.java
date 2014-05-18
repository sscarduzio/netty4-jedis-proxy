package eu.codesigner.redis;

import io.netty.channel.ChannelHandlerContext;
import redis.server.netty.RedisServer;

public interface ExtendedRedisServer extends RedisServer{
  public void subscribe(ChannelHandlerContext ctx, byte[][] channels);

  public void unsubscribe(ChannelHandlerContext ctx, byte[][] channels);

  public void punsubscribe(ChannelHandlerContext ctx, byte[][] channels);
}
