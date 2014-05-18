Redis multiplexer
==================
Accepts large number of client connections on the netty4 side.

Funnels (multiplexes) all the commands towards Redis through a small Jedis threadpool.

#### Abstract
Makes sense that we handle asynchronously (netty4) the potentially numerous clients.

It's just fine to handle synchronously (Jedis) a small number of persistent connections.

Of course, **the blocking code does not block netty event loop**. Jedis is always called from a separated thread pool.

### Build Instructions
This project makes extensive use of lambdas, so **Java 8** is required.

1. clone Sam Pullara's ```redis-protocol``` project (see credits)
2. mvn -DskipTests install
3. clone this repo
4. mvn package
5. ```java -jar target/netty4-jedis-proxy-0.0.1.jar -cp target/lib/ ```

### Usage
```
  -listenPort (-l) [Integer] Listen port (1234)
  -redisPort (-p) [Integer] Redis server port (6379)
  -redisHost (-h) [String] Redis server host (localhost)
  -jconnectTimeout (-t) [int] jedis connection timeout (2000)
  -jMinIdle (-n) [int] jedis minimum connection idle time (0)
  -jMaxIdle (-m) [int] jedis maximum connection idle time (8)
```

### Warnings
* This is still experimental, many commands are supported, but less frequently used commands may not yet be implemented.
* PubSub: handle with care, it uses one thread for each blocked connection. Redis-side connection is correctly released if client-side connection breaks.

### Todo
* Port the rest of the non blocking commands to Jedis
* Port the rest of the blocking commands to Jedis (BLPOP, BRPOP, ... )

### Credits
* Netty project: http://netty.io
* Sam Pullara's redis protocol for Netty: https://github.com/spullara/redis-protocol
