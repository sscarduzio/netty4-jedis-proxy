package eu.codesigner.redis;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.InlineReply;
import redis.netty4.Reply;
import redis.server.netty.RedisException;
import redis.server.netty.RedisServer;
import redis.util.BytesKey;

import com.google.common.base.Charsets;

/**
 * Handle decoded commands
 */
@ChannelHandler.Sharable
public class OffLoopRedisCommandHandler extends SimpleChannelInboundHandler<Command> {
  private static final ExecutorService                executorService     = Executors.newCachedThreadPool();
  public static final AttributeKey<BinaryJedisPubSub> PUBSUB_JEDIS = AttributeKey.valueOf("blockedJedis");

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    BinaryJedisPubSub j = ctx.attr(PUBSUB_JEDIS).get();
    if (j != null) {
      j.unsubscribe();
    }

    ctx.fireChannelInactive();
  }

  private Map<BytesKey, Wrapper> methods = new HashMap<BytesKey, Wrapper>();

  interface Wrapper {
    Reply execute(ChannelHandlerContext ctx, Command command) throws RedisException;
  }

  public OffLoopRedisCommandHandler(final ExtendedRedisServer rs) {
    Class<? extends RedisServer> aClass = rs.getClass();
    for (final Method method : aClass.getMethods()) {
      final Class<?>[] types = method.getParameterTypes();
      methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
        @Override
        public Reply execute(ChannelHandlerContext ctx, Command command) throws RedisException {
          Object[] objects = new Object[types.length];
          try {
            command.toArguments(objects, types);
            return (Reply) method.invoke(rs, objects);
          }
          catch (IllegalAccessException e) {
            throw new RedisException("Invalid server implementation");
          }
          catch (InvocationTargetException e) {
            Throwable te = e.getTargetException();
            if (!(te instanceof RedisException)) {
              te.printStackTrace();
            }
            return new ErrorReply("ERR " + te.getMessage());
          }
          catch (Exception e) {
            return new ErrorReply("ERR " + e.getMessage());
          }
        }
      });
      
      Wrapper subscriberWrapper =  new Wrapper() {

        @Override
        public Reply execute(ChannelHandlerContext ctx, Command command) throws RedisException {
          try {
            Field f = command.getClass().getDeclaredField("objects");
            f.setAccessible(true);
            Object[] ojs;
            ojs = (Object[]) f.get(command);
            byte[][] chans = (byte[][]) Arrays.copyOfRange(ojs, 1, ojs.length);
            rs.subscribe(ctx, chans);

            return null;
          }
          catch (Exception e) {
            e.printStackTrace();
            return null;
          }
        }
      };
      methods.put(new BytesKey("subscribe".getBytes()),subscriberWrapper);
      methods.put(new BytesKey("psubscribe".getBytes()),subscriberWrapper);
    }
  }

  private static final byte LOWER_DIFF = 'a' - 'A';
 
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
    // Handle this off-eventloop, in a cached thread from executor service
    CompletableFuture.supplyAsync(() -> {
      try {
        doChannelRead0(ctx, msg);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }, executorService);

  }

  protected void doChannelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
    byte[] name = msg.getName();
    for (int i = 0; i < name.length; i++) {
      byte b = name[i];
      if (b >= 'A' && b <= 'Z') {
        name[i] = (byte) (b + LOWER_DIFF);
      }
    }
    Wrapper wrapper = methods.get(new BytesKey(name));
    Reply reply;
    if (wrapper == null) {
      reply = new ErrorReply("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
    }
    else {
      reply = wrapper.execute(ctx, msg);
    }
    if (reply == QUIT) {
      ctx.close();
    }
    else {
      if (msg.isInline()) {
        if (reply == null) {
          reply = new InlineReply(null);
        }
        else {
          reply = new InlineReply(reply.data());
        }
      }
      if (reply == null) {
        reply = NYI_REPLY;
      }
      
      final Reply theReply = reply;
      
      // Always write from the event loop, minimize the wakeup events
      ctx.channel().eventLoop().execute(new Runnable() {
        
        @Override
        public void run() {
          // Not interested in the channel promise
          ctx.writeAndFlush(theReply, ctx.channel().voidPromise());
        }
      });
    }
  }
}
