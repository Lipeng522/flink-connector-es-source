## Flink-connector-es-source

#### 1、使用方式

```sql
create table if not exists table_name
(
     key                 string
    ,value			     string
    ,primary key (key) not enforced   -- 必须
)comment '表描述'
with (
   'connector' = 'es',
   'format' = 'json',
   'hosts' = 'host',
   'index' = 'index_name',
   'document-type' = 'type_name',
   'lookup.cache.max-rows' = '50',
   'lookup.cache.ttl' = '1000'
);
```



#### 2、参数设置

| 参数                  | 说明        | 是否必填     | 取值          |
| --------------------- | ----------- | ------------ | ------------- |
| connector             | 连接标识    | 是           | es            |
| host                  | 主机名      | 是           | 如：127.0.0.1 |
| format                | 格式化类型  | 是           | 如：json      |
| index                 | index名     | 是           |               |
| document-type         | index的type | 是           | 如：type      |
| lookup.cache.max-rows | 缓存的行数  | 否（默认-1） | 如：100       |
| lookup.cache.ttl      | ttl时间设置 | 否（默认10） | 如：100L      |
| lookup_max_retries    | 重试次数    | 否（默认3）  | 如：10        |

备注：lookup.cache.max-rows和lookup.cache.ttl 都不为-1时启用缓存

#### 3、版本

###### es版本为：6.8.12