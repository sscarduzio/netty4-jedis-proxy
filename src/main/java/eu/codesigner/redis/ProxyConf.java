package eu.codesigner.redis;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

public class ProxyConf {
  @Argument(alias = "l")
  public static Integer listenPort = 5000;

  @Argument(alias = "p")
  public static Integer redisPort = 6379;

  @Argument(alias = "h")
  public static String redisHost = "localhost";
  
  @Argument(alias = "t")
  public static int jconnectTimeout = 2000;

  @Argument(alias = "n")
  public static int jMinIdle = 0;

  @Argument(alias = "m")
  public static int jMaxIdle = 8;

  public static void init(String[] args) {
    try {
      Args.parse(ProxyConf.class, args);
    } catch (IllegalArgumentException e) {
      Args.usage(ProxyConf.class);
      System.exit(1);
    }
    
  }
}
