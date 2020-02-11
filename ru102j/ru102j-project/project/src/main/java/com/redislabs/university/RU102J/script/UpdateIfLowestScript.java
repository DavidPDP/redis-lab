package com.redislabs.university.RU102J.script;

import java.util.Collections;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class UpdateIfLowestScript {
	
	private final String sha;
	private static String script = 
			"local key = KEYS[1] " +
            "local new = ARGV[1] " +
			"local current = redis.call('GET',key) " +
            "if (current == false) or " +
			"(tonumber(new) < tonumber(current)) then " + 
            "redis.call('SET',key,new) " + 
			"return 1 " + 
            "else " + 
			"return 0" +
            "end";
	
	public UpdateIfLowestScript(JedisPool jedisPool) {
		try(Jedis jedis = jedisPool.getResource()){
			this.sha = jedis.scriptLoad(script);
		}
	}
	
	public boolean updateIfLowest(Transaction jedis, String key, Integer newValue) {
		List<String> keys = Collections.singletonList(key);
		List<String> args = Collections.singletonList(String.valueOf(newValue));
		Object response = jedis.evalsha(sha, keys, args);
		return (Long)response == 1;
	}
	
}
