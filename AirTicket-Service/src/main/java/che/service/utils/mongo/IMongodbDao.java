package che.service.utils.mongo;


import org.bson.conversions.Bson;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Map;

/**
 * @author Tom
 */
public interface IMongodbDao<T extends BaseMongoEntity<?>> {
    /**
     * @param dbName
     * @param entity
     * @return
     */
    public boolean insertOne(String dbName, T entity);

    /**
     * @param dbName
     * @param list
     * @return
     */
    public boolean insertMany(String dbName, List<T> list);

    /**
     * @param dbName
     * @param param
     * @param newEntity
     * @return
     */
    @Deprecated
    public boolean updateOne(String dbName, Map<String, Object> param, T newEntity);

    /**
     * @param dbName
     * @param param
     * @param newEntity
     * @return
     */
    @Deprecated
    public boolean updateMany(String dbName, Map<String, Object> param, T newEntity);

    /**
     * @param dbName
     * @param param
     * @return
     */
    public boolean deleteOne(String dbName, Map<String, Object> param);

    /**
     * @param dbName
     * @param param
     * @return
     */
    public boolean deleteMany(String dbName, Map<String, Object> param);

    /**
     * @param dbName
     * @param id
     * @return
     */
    public T getById(String dbName, String id);

    /**
     * @param dbName
     * @param ids
     * @return
     */
    public List<T> findByIds(String dbName, List<String> ids);

    /**
     * @param dbName
     * @param param
     * @return
     */
    public List<T> findByMap(String dbName, Map<String, Object> param);

    /**
     * @param dbName
     * @param listBson
     * @return
     */
    public List<T> findByFilter(String dbName, List<Bson> listBson);

    /**
     * @param dbName
     * @param listBson
     * @param orderBy
     * @return
     */
    public List<T> findByFilter(String dbName, List<Bson> listBson, Bson orderBy);

    /**
     * @param dbName
     * @param listBson
     * @return
     */
    public List<T> findByFilterOr(String dbName, List<Bson> listBson);

    /**
     * @param dbName
     * @param listBson
     * @param orderBy
     * @return
     */
    public List<T> findByFilterOr(String dbName, List<Bson> listBson, Bson orderBy);

    /**
     * @param dbName
     * @param listBson
     * @param orderBy
     * @return
     */
    public List<T> findByFilterLimitOne(String dbName, List<Bson> listBson, Bson orderBy);

    /**
     * @param dbName
     * @param entity
     * @return
     */
    @Deprecated
    public List<T> findByEntity(String dbName, T entity);

    /**
     * @param dbName
     * @param param
     * @param page
     * @return
     */
    public Page<T> searchByMap(String dbName, Map<String, Object> param, Page<T> page);

    /**
     * @param dbName
     * @param listBson
     * @param page
     * @return
     */
    public Page<T> searchByFilter(String dbName, List<Bson> listBson, Page<T> page);

    /**
     * @param dbName
     * @param listBson
     * @param page
     * @return
     */
    public Page<T> searchByFilterOr(String dbName, List<Bson> listBson, Page<T> page);

}
