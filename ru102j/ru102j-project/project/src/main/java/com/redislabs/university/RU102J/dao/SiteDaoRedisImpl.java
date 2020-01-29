package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
    	Set<Site> sites = new HashSet<Site>();
    	try(Jedis jedis = jedisPool.getResource()){
    		String siteHashKey = RedisSchema.getSiteHashKey(); 
    		ScanParams scanParams = new ScanParams().count(10).match(siteHashKey + "*"); //count for default is 10
    		String cursor = ScanParams.SCAN_POINTER_START;
    		ScanResult<String> scanResult;
    		do {
    			scanResult = jedis.scan(cursor,scanParams);
    			List<String> results = scanResult.getResult();
    			if(!results.isEmpty()) {
    				for (String site : results) {
						Map<String, String> fields = jedis.hgetAll(site);
    					sites.add(new Site(fields));
					}
    			}
    			cursor = scanResult.getCursor();
    		} while(!cursor.equals(ScanParams.SCAN_POINTER_START));
    	}
        return sites;
    }
}
