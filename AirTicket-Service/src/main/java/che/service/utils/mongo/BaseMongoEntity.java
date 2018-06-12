package che.service.utils.mongo;

import che.service.utils.reflection.Reflections;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bson.Document;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * <p>
 * Description: 封装mongodb与javaBean互转的方法
 * <p>
 * Date: 2016/4/8 14:33
 * <p>
 * Version: 1.0
 *
 * @author Tom
 */
public abstract class BaseMongoEntity<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Document转实体
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public T toEntity(Document document) {

        for (String key : document.keySet()) {
            if (key.equals("_id")) {
                continue;
            }
            Object object = document.get(key);
            if (object != null) {
                //2017年9月19日09:29:58 加上异常处理 如果反射出错(mongo中有,但是实体还未更新,会找不到setter方法) 继续执行
                try {
                    Reflections.invokeSetter(this, key, object);
                } catch (Exception e) {
                }
            }


        }
        return (T) this;
    }

    /**
     * 实体转document
     *
     * @return
     */
    public Document toDocument() {

        Document document = new Document();

        // 获取类中的所有定义字段
        Field[] fields = this.getClass().getDeclaredFields();

        // 循环遍历字段，获取字段对应的属性值
        for (Field field : fields) {

            try {
                field.setAccessible(true);
                String fieldName = field.getName();
                String newFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
                Object value = field.get(this);
                if (value == null) {
                    // value="";
                    continue;
                }
                if (!fieldName.equals("serialVersionUID")) {
                    document.put(newFieldName, value);
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return document;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
