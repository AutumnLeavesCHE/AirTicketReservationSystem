package che.service.utils.mongo;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import che.service.utils.page.Page;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @param <T>
 * @author jiaoanjian
 */
public abstract class MongodbDao<T extends BaseMongoEntity<?>> implements IMongodbDao<T> {

    /**
     * 插入一条记录
     *
     * @param dbName
     * @param entity
     * @return
     */
    @Override
    public boolean insertOne(String dbName, T entity) {
        if (entity == null) {
            return false;
        }
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.insertOne(dbName, clazz, entity.toDocument());
    }

    /**
     * 插入多条记录
     *
     * @param dbName
     * @param list
     * @return
     */
    @Override
    public boolean insertMany(String dbName, List<T> list) {
        if (list.isEmpty()) {
            return false;
        }
        Class<T> clazz = this.getTClass();
        List<Document> docList = Lists.newArrayList();
        for (T entity : list) {
            docList.add(entity.toDocument());
        }
        return MongoDBUtil.insertMany(dbName, clazz, docList);
    }

    /**
     * 更新第一条匹配的记录
     *
     * @param dbName
     * @param param     将匹配的条件放入map中
     * @param newEntity
     * @return
     */
    @Override
    public boolean updateOne(String dbName, Map<String, Object> param, T newEntity) {
        Class<T> clazz = this.getTClass();
        Document doc = new Document();
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        return MongoDBUtil.updateOne(dbName, clazz, doc, newEntity.toDocument());
    }

    /**
     * 更新所有匹配的记录
     *
     * @param dbName
     * @param param     将匹配的条件放入map中
     * @param newEntity
     * @return
     */
    @Override
    public boolean updateMany(String dbName, Map<String, Object> param, T newEntity) {
        Class<T> clazz = this.getTClass();
        Document doc = new Document();
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        return MongoDBUtil.updateMany(dbName, clazz, doc, newEntity.toDocument());
    }

    /**
     * 删除第一条匹配的记录
     *
     * @param dbName
     * @param param
     * @return
     */
    @Override
    public boolean deleteOne(String dbName, Map<String, Object> param) {
        Class<T> clazz = this.getTClass();
        Document doc = new Document();
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        return MongoDBUtil.deleteOne(dbName, clazz, doc);
    }

    /**
     * 删除所有匹配的记录
     *
     * @param dbName
     * @param param
     * @return
     */
    @Override
    public boolean deleteMany(String dbName, Map<String, Object> param) {
        Class<T> clazz = this.getTClass();
        Document doc = new Document();
        if (param != null) {
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                doc.append(entry.getKey(), entry.getValue());
            }
        }
        return MongoDBUtil.deleteMany(dbName, clazz, doc);
    }


    /**
     * 根据主键查询
     *
     * @param dbName
     * @param id
     * @return
     */
    @Override
    public T getById(String dbName, String id) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.getOne(dbName, clazz, new Document("Id", id));
    }

    /**
     * 根据多个id 那多条记录
     *
     * @param dbName
     * @param ids
     * @return
     */
    @Override
    public List<T> findByIds(String dbName, List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return Lists.newArrayList();
        }

        Class<T> clazz = this.getTClass();
        List<Bson> bsonList = new ArrayList<Bson>();
        for (String id : ids) {
            bsonList.add(Filters.eq("Id", id));
        }
        return MongoDBUtil.findByFilterOr(dbName, clazz, bsonList);
    }


    /**
     * 根据map查询列表
     *
     * @param dbName
     * @param param
     * @return
     */
    @Override
    public List<T> findByMap(String dbName, Map<String, Object> param) {
        Class<T> clazz = this.getTClass();
        Document document = new Document();
        if (param != null) {
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                document.append(entry.getKey(), entry.getValue());
            }
        }
        return MongoDBUtil.find(dbName, clazz, document);
    }

    /**
     * 根据Filter查询
     *
     * @param dbName
     * @return
     */
    @Override
    public List<T> findByFilter(String dbName, List<Bson> listBson) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.findByFilter(dbName, clazz, listBson);
    }

    /**
     * 根据Filter查询
     *
     * @param dbName
     * @return
     */
    @Override
    public List<T> findByFilter(String dbName, List<Bson> listBson, Bson orderBy) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.findByFilter(dbName, clazz, listBson, orderBy);
    }

    @Override
    public List<T> findByFilterOr(String dbName, List<Bson> listBson) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.findByFilterOr(dbName, clazz, listBson);
    }

    @Override
    public List<T> findByFilterOr(String dbName, List<Bson> listBson, Bson orderBy) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.findByFilterOr(dbName, clazz, listBson, orderBy);
    }

    /**
     * 根据Filter查询一个
     *
     * @param dbName
     * @param listBson
     * @param orderBy
     * @return
     */
    @Override
    public List<T> findByFilterLimitOne(String dbName, List<Bson> listBson, Bson orderBy) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.findByFilterLimitOne(dbName, clazz, listBson, orderBy);
    }

    /**
     * 根据实体查询列表（注意，此时所有不为空实体属性都会作为条件进行搜索）
     *
     * @param dbName
     * @param entity
     * @return
     */
    @Override
    public List<T> findByEntity(String dbName, T entity) {
        Class<T> clazz = this.getTClass();
        return MongoDBUtil.find(dbName, clazz, entity.toDocument());
    }

    /**
     * 分页查询，目前page中没有总页数
     *
     * @param dbName
     * @param param
     * @param page
     * @return
     */
    public Page<T> searchByMap(String dbName, Map<String, Object> param, Page<T> page) {
        Class<T> clazz = this.getTClass();
        Bson orderBy;
        if (StringUtils.isNotBlank(page.getOrderBy())) {
            orderBy = new BasicDBObject(page.getOrderBy(), page.getOrderByRule());
        } else {
            orderBy = new BasicDBObject("CreateTime", page.getOrderByRule());//1 or -1 倒序
        }
        List<Document> docList = Lists.newArrayList();
        if (param != null) {
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                docList.add(new Document(entry.getKey(), entry.getValue()));
            }
        }

        List<T> list = MongoDBUtil.findByPage(dbName, clazz, page.getPageNo(), page.getPageSize(), orderBy, docList);
        long count = MongoDBUtil.getCountByDocument(dbName, clazz, docList);
        page.setList(list);
        page.setCount(count);
        return page;
    }

    public Page<T> searchByFilter(String dbName, List<Bson> listBson, Page<T> page) {
        Class<T> clazz = this.getTClass();
        Bson orderBy;
        if (StringUtils.isNotBlank(page.getOrderBy())) {
            orderBy = new BasicDBObject(page.getOrderBy(), page.getOrderByRule());
        } else {
            orderBy = new BasicDBObject("CreateTime", page.getOrderByRule());//1 or -1 倒序
        }

        Page<T> returnPage = MongoDBUtil.searchPageByFilter(dbName, clazz, page, orderBy, listBson);
        long count = MongoDBUtil.getCountByFilter(dbName, clazz, listBson);
        returnPage.setCount(count);
        return returnPage;
    }

    public Page<T> searchByFilterOr(String dbName, List<Bson> listBson, Page<T> page) {
        Class<T> clazz = this.getTClass();
        Bson orderBy;
        if (StringUtils.isNotBlank(page.getOrderBy())) {
            orderBy = new BasicDBObject(page.getOrderBy(), page.getOrderByRule());
        } else {
            orderBy = new BasicDBObject("CreateTime", page.getOrderByRule());//1 or -1 倒序
        }

        Page<T> returnPage = MongoDBUtil.searchPageByFilterOr(dbName, clazz, page, orderBy, listBson);
        long count = MongoDBUtil.getCountByFilterOr(dbName, clazz, listBson);
        returnPage.setCount(count);
        return returnPage;
    }


    private Class<T> getTClass() {
        @SuppressWarnings("unchecked")
        Class<T> tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        return tClass;
    }

}
