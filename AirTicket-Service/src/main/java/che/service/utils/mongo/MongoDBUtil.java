package che.service.utils.mongo;

import che.service.utils.page.Page;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MongoDB工具类 Mongo实例代表了一个数据库连接池，即使在多线程的环境中，一个Mongo实例对我们来说已经足够了<br>
 * 注意Mongo已经实现了连接池，并且是线程安全的。 <br>
 * 设计为单例模式， 因 MongoDB的Java驱动是线程安全的，对于一般的应用，只要一个Mongo实例即可，<br>
 * Mongo有个内置的连接池（默认为10个） 对于有大量写和读的环境中，为了确保在一个Session中使用同一个DB时，<br>
 * DB和DBCollection是绝对线程安全的<br>
 *
 */

@Component
public class MongoDBUtil implements InitializingBean {

	protected static Logger logger = LoggerFactory.getLogger(MongoDBUtil.class);

	public static final int ASC = 1;

	public static final int DESC = -1;

	private static MongoClient mongoClient;

	@Resource
	private Environment env;

//	static {
//
//		logger.debug("===============MongoDBUtil初始化================");
//
//		ResourceBundle rb = ResourceBundle.getBundle("application");
//
//		String springBootInclude=rb.getString("spring.profiles.include");
//
//		if(springBootInclude.contains("production")){
//			rb = ResourceBundle.getBundle("application-production");
//		}else{
//			rb = ResourceBundle.getBundle("application-dev");
//		}
//
//		String hostandport = rb.getString("mongos.host");
//
//		List<ServerAddress> sdList = new ArrayList<ServerAddress>();
//
//		for (String item : hostandport.split(",")) {
//			String host = item.split(":")[0].trim();
//			int port = Integer.valueOf(item.split(":")[1].trim());
//			sdList.add(new ServerAddress(host, port));
//		}
//		MongoClientOptions.Builder options = new MongoClientOptions.Builder();
//		options.connectionsPerHost(300);// 连接池设置为300个连接,默认为100
//		options.connectTimeout(15000);// 连接超时，推荐>3000毫秒
//		options.maxWaitTime(5000); //
//		options.socketTimeout(0);// 套接字超时时间，0无限制
//		options.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out
//																	// of
//																	// semaphores
//																	// to get
//																	// db”错误。
//		options.build();
//		mongoClient = new MongoClient(sdList, options.build());
//
//	}

	// ------------------------------------共用方法---------------------------------------------------
	/**
	 * 获取DB实例 - 指定DB
	 *
	 * @param dbName
	 * @return
	 */
	public static MongoDatabase getDB(String dbName) {
		if (StringUtils.isNotBlank(dbName)) {
			return mongoClient.getDatabase(dbName);
		}
		return null;
	}

	/**
	 * 获取collection对象 - 指定Collection
	 *
	 * @param collName
	 * @return
	 */
	public static MongoCollection<Document> getCollection(String dbName, String collectionName) {
		if (StringUtils.isBlank(dbName) || StringUtils.isBlank(collectionName)) {
			return null;
		}
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		return collection;
	}

	public static boolean insertOne(String dbName, Class<?> clazz, Document document) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			coll.insertOne(document);
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	};

	public static boolean insertMany(String dbName, Class<?> clazz, List<Document> documentList) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			coll.insertMany(documentList);
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	};

	/**
	 * 根据 key里的 字段 更新 newDocument
	 * 
	 * @param dbName
	 * @param key
	 * @param newDocument
	 * @param clazz
	 * @return
	 */
	public static boolean updateOne(String dbName, Class<?> clazz, Document key, Document newDocument) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			collection.updateOne(key, new Document("$set", newDocument));
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * 这个跟updateOne的区别是 会根据key里多个字段的条件 更改多条记录
	 *
	 * 根据 key里的 字段 更新 newDocument
	 * 
	 * @param dbName
	 * @param key
	 * @param newDocument
	 * @param clazz
	 * @return
	 */
	public static boolean updateMany(String dbName, Class<?> clazz, Document key, Document newDocument) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			coll.updateMany(key, new Document("$set", newDocument));
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * 删除一个
	 * 
	 * @param dbName
	 * @param document
	 * @param clazz
	 * @return
	 */
	public static boolean deleteOne(String dbName, Class<?> clazz, Document document) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			coll.deleteOne(document);
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * 删除所有匹配的
	 * 
	 * @param dbName
	 * @param clazz
	 * @param document
	 * @return
	 */
	public static boolean deleteMany(String dbName, Class<?> clazz, Document document) {
		String tableName = getTableName(clazz);
		try {
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			coll.deleteMany(document);
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			return false;
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> T getOne(String dbName, Class<?> clazz, Document document) {
		String tableName = getTableName(clazz);
		MongoCollection<Document> collection = getCollection(dbName, tableName);
		Document findFirst = collection.find(document).first();
		T entity;
		try {
			if(findFirst==null) return null;
			entity = (T) clazz.newInstance();
			return (T) entity.toEntity(findFirst);
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 根据 实体里的赋值字段 查询
	 *
	 */
	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> find(String dbName, Class<?> clazz, Document document) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable = collection.find(document);
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> findByFilter(String dbName, Class<?> clazz, List<Bson> listBson) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = collection.find(Filters.and(listBson));
			} else {
				findIterable = collection.find();
			}
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> findByFilter(String dbName, Class<?> clazz, List<Bson> listBson, Bson orderBy) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = collection.find(Filters.and(listBson)).sort(orderBy);
			} else {
				findIterable = collection.find().sort(orderBy);
			}
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * or查询 区别于上面的and查询
	 * 
	 * @param dbName
	 * @param clazz
	 * @param listBson
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> findByFilterOr(String dbName, Class<?> clazz, List<Bson> listBson) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = collection.find(Filters.or(listBson));
			} else {
				findIterable = collection.find();
			}
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> findByFilterOr(String dbName, Class<?> clazz, List<Bson> listBson, Bson orderBy) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = collection.find(Filters.or(listBson)).sort(orderBy);
			} else {
				findIterable = collection.find().sort(orderBy);
			}
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> List<T> findByFilterLimitOne(String dbName, Class<?> clazz, List<Bson> listBson, Bson orderBy) {
		String tableName = getTableName(clazz);
		List<T> list = null;
		try {
			MongoCollection<Document> collection = getCollection(dbName, tableName);
			FindIterable<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = collection.find(Filters.and(listBson)).sort(orderBy).limit(1);
			} else {
				findIterable = collection.find().sort(orderBy).limit(1);
			}
			list = new ArrayList<T>();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				Document value = mongoCursor.next();
				T entity = (T) clazz.newInstance();
				list.add((T) entity.toEntity(value));
			}
			mongoCursor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 分页基本 查询
	 *
	 * @param dbName
	 * @param pageNo
	 * @param pageSize
	 * @param clazz
	 * @param documentList
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Deprecated //查询有bug  多个相同key会覆盖
	public static <T extends BaseMongoEntity<?>> List<T> findByPage(String dbName, Class<?> clazz, int pageNo, int pageSize, Bson sortBy, List<Document> documentList) {
		String tableName = getTableName(clazz);
		List<T> returnList = null;
		MongoCollection<Document> coll = null;
		try {
			returnList = new ArrayList<T>();
			coll = getCollection(dbName, tableName);
			// Bson orderBy = new BasicDBObject("LogCreateDate", 1);
			Map<String, Object> map = new HashMap<String, Object>();
			for (Document d : documentList) {
				for (String key : d.keySet()) {
					map.put(key, d.get(key));
				}
			}


			MongoCursor<Document> it = coll.find(new Document(map)).sort(sortBy).skip((pageNo - 1) * pageSize).limit(pageSize).iterator();

			while (it.hasNext()) {
				Document value = it.next();
				T entity = (T) clazz.newInstance();
				returnList.add((T) entity.toEntity(value));
			}
			it.close();
		} catch (Exception e) {
			return null;
		}
		return returnList;
	}

	/**
	 * 根据条件获得数据总数
	 *
	 * @param dbName
	 * @param clazz
	 * @param documentList
	 * @return
	 */
	public static long getCountByDocument(String dbName,Class<?> clazz, List<Document> documentList){
		String tableName = getTableName(clazz);
		MongoCollection<Document> coll = null;
		long count=0;
		try {
			coll = getCollection(dbName, tableName);
			Map<String, Object> map = new HashMap<String, Object>();
			for (Document d : documentList) {
				for (String key : d.keySet()) {
					map.put(key, d.get(key));
				}
			}
			count= coll.count(new Document(map));

		} catch (Exception e) {
			return 0;
		}
		return count;
	}

	/**
	 *
	 * 根据条件获得数据总数
	 *
	 * @param dbName
	 * @param clazz
	 * @param listBson
	 * @return
	 */
	public static long getCountByFilter(String dbName,Class<?> clazz, List<Bson> listBson){
		String tableName = getTableName(clazz);
		MongoCollection<Document> coll = null;
		long count=0;
		try {
			coll = getCollection(dbName, tableName);
			if(listBson==null || listBson.isEmpty()){
				count= coll.count();
			}else {
				count= coll.count(Filters.and(listBson));
			}

		} catch (Exception e) {
			return 0;
		}
		return count;
	}
	
	/**
	 *
	 * 根据条件获得数据总数(Or)
	 *
	 * @param dbName
	 * @param clazz
	 * @param listBson
	 * @return
	 */
	public static long getCountByFilterOr(String dbName,Class<?> clazz, List<Bson> listBson){
		String tableName = getTableName(clazz);
		MongoCollection<Document> coll = null;
		long count=0;
		try {
			coll = getCollection(dbName, tableName);
			if(listBson==null || listBson.isEmpty()){
				count= coll.count();
			}else {
				count= coll.count(Filters.or(listBson));
			}

		} catch (Exception e) {
			return 0;
		}
		return count;
	}

	/**
	 * 分页基本 查询
	 *
	 * @param dbName
	 * @param pageNo
	 * @param pageSize
	 * @param clazz
	 * @param documentList
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> Page<T> searchPageByFilter(String dbName, Class<?> clazz, Page<T> page, Bson orderBy, List<Bson> listBson) {
		String tableName = getTableName(clazz);
		List<T> returnList = null;
		try {
			returnList = new ArrayList<T>();
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			MongoCursor<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = coll.find(Filters.and(listBson)).sort(orderBy).skip((page.getPageNo() - 1) * page.getPageSize()).limit(page.getPageSize()).iterator();
			} else {
				findIterable = coll.find().sort(orderBy).skip((page.getPageNo() - 1) * page.getPageSize()).limit(page.getPageSize()).iterator();
			}
			while (findIterable.hasNext()) {
				Document value = findIterable.next();
				T entity = (T) clazz.newInstance();
				returnList.add((T) entity.toEntity(value));
			}
			findIterable.close();
			page.setList(returnList);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			e.printStackTrace();
			return null;
		}
		return page;
	}
	
	/**
	 * 分页基本 查询
	 *
	 * @param dbName
	 * @param pageNo
	 * @param pageSize
	 * @param clazz
	 * @param documentList
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends BaseMongoEntity<?>> Page<T> searchPageByFilterOr(String dbName, Class<?> clazz, Page<T> page, Bson orderBy, List<Bson> listBson) {
		String tableName = getTableName(clazz);
		List<T> returnList = null;
		try {
			returnList = new ArrayList<T>();
			MongoCollection<Document> coll = getCollection(dbName, tableName);
			MongoCursor<Document> findIterable;
			if (listBson != null && listBson.size() > 0) {
				findIterable = coll.find(Filters.or(listBson)).sort(orderBy).skip((page.getPageNo() - 1) * page.getPageSize()).limit(page.getPageSize()).iterator();
			} else {
				findIterable = coll.find().sort(orderBy).skip((page.getPageNo() - 1) * page.getPageSize()).limit(page.getPageSize()).iterator();
			}
			while (findIterable.hasNext()) {
				Document value = findIterable.next();
				T entity = (T) clazz.newInstance();
				returnList.add((T) entity.toEntity(value));
			}
			findIterable.close();
			page.setList(returnList);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			e.printStackTrace();
			return null;
		}
		return page;
	}

	/**
	 * 根据注解 获得实体代表的表名
	 * 
	 * @param clazz
	 * @return
	 */
	private static String getTableName(Class<?> clazz) {
		String tableName = null;
		if (clazz.isAnnotationPresent(Table.class)) {
			Table table = (Table) clazz.getAnnotation(Table.class);
			tableName = table.name();
		} else {
			tableName = clazz.getSimpleName();
		}
		return tableName;
	}

	/**
	 * 拼装 小于 查询的Document
	 *
	 * @param column
	 * @param value
	 * @return
	 */
	public static Document lessThan(String column, Object value) {

		return new Document(column, new Document("$lt", value));
	}

	/**
	 * 拼装小于等于的Document
	 * 
	 * @param column
	 * @param value
	 * @return
	 */
	public static Document lessThanE(String column, Object value) {

		return new Document(column, new Document("$lte", value));
	}

	/**
	 * 拼装 大于 查询语句的Document
	 *
	 * @param column
	 * @param value
	 * @return
	 */
	public static Document greaterThan(String column, Object value) {

		return new Document(column, new Document("$gt", value));
	}

	/**
	 * 拼装大于等于的Document
	 * 
	 * @param column
	 * @param value
	 * @return
	 */
	public static Document greaterThanE(String column, Object value) {

		return new Document(column, new Document("$gte", value));
	}

	/**
	 * 关闭Mongodb
	 */
	public static void close() {
		if (mongoClient != null) {
			mongoClient.close();
			mongoClient = null;
		}
	}

	/**
	 * 测试入口
	 *
	 * @param args
	 */
	public static void main(String[] args) {

		///////////////////////////////////////////

		//不能跑main方法 只能在spring容器里跑

		///////////////////////////////////////////

	}

	@Override
	public void afterPropertiesSet() throws Exception {

		logger.warn("=================新版初始化mongodb，只能在spring容器里使用=============================");

		String hostandport = env.getProperty("mongos.host");
		String maxConns = env.getProperty("mongos.maxConns");
		String connectTimeoutStr = env.getProperty("mongos.connectTimeout");
		String maxWaitTimeStr = env.getProperty("mongos.maxWaitTime");
		String socketTimeoutStr = env.getProperty("mongos.socketTimeout");
		String threadNumberStr = env.getProperty("mongos.threadNumber");

		int connectionsPerHost = 150; //默认值
		int connectTimeout = 15000;
		int maxWaitTime = 5000;
		int socketTimeout = 0;
		int threadNumber = 5000;

		if(StringUtils.isNotBlank(maxConns)){
			connectionsPerHost = Integer.parseInt(maxConns);
		}
		if(StringUtils.isNotBlank(connectTimeoutStr)){
			connectTimeout = Integer.parseInt(connectTimeoutStr);
		}
		if(StringUtils.isNotBlank(maxWaitTimeStr)){
			maxWaitTime = Integer.parseInt(maxWaitTimeStr);
		}
		if(StringUtils.isNotBlank(socketTimeoutStr)){
			socketTimeout = Integer.parseInt(socketTimeoutStr);
		}
		if(StringUtils.isNotBlank(threadNumberStr)){
			threadNumber = Integer.parseInt(threadNumberStr);
		}

		List<ServerAddress> sdList = new ArrayList<ServerAddress>();

		for (String item : hostandport.split(",")) {
			String host = item.split(":")[0].trim();
			int port = Integer.valueOf(item.split(":")[1].trim());
			sdList.add(new ServerAddress(host, port));
		}
		MongoClientOptions.Builder options = new MongoClientOptions.Builder();
		options.connectionsPerHost(connectionsPerHost);// 连接池设置为300个连接,默认为100
		options.connectTimeout(connectTimeout);// 连接超时，推荐>3000毫秒
		options.maxWaitTime(maxWaitTime); //
		options.socketTimeout(socketTimeout);// 套接字超时时间，0无限制
		options.threadsAllowedToBlockForConnectionMultiplier(threadNumber);// 线程队列数，如果连接线程排满了队列就会抛出“Out
		// of
		// semaphores
		// to get
		// db”错误。
		options.build();
		mongoClient = new MongoClient(sdList, options.build());

	}
}