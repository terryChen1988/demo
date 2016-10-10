package hadoop_test;

import redis.clients.jedis.Jedis;

public class TestRedis {

	public static void main(String[] args) {
		Jedis jedis = new Jedis("localhost", 6379);
		
		System.out.println(jedis.get("foo"));
		
	}

}
