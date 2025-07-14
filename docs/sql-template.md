# SQL 生成规范

本规范用于指导如何根据分析请求生成标准 SQL 语句，适用于数据分析、可视化等场景。

## 1. 支持的分析操作类型及 SQL 模板

### 1.1 数值字段分析

| 操作类型    | 说明       | SQL 模板示例                                                                                                                                                                                                                           |
| ----------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AVG         | 平均值     | SELECT AVG({column}) as avg\_{column} FROM {table}                                                                                                                                                                                     |
| MAX         | 最大值     | SELECT MAX({column}) as max\_{column} FROM {table}                                                                                                                                                                                     |
| MIN         | 最小值     | SELECT MIN({column}) as min\_{column} FROM {table}                                                                                                                                                                                     |
| SUM         | 求和       | SELECT SUM({column}) as sum\_{column} FROM {table}                                                                                                                                                                                     |
| VAR_POP     | 总体方差   | SELECT VAR*POP({column}) as var_pop*{column} FROM {table}                                                                                                                                                                              |
| STDDEV_POP  | 总体标准差 | SELECT STDDEV*POP({column}) as stddev_pop*{column} FROM {table}                                                                                                                                                                        |
| COUNT       | 计数       | SELECT COUNT({column}) as count\_{column} FROM {table}                                                                                                                                                                                 |
| MEDIAN      | 中位数     | SELECT PERCENTILE*CONT(0.5) WITHIN GROUP (ORDER BY {column}) as median*{column} FROM {table}                                                                                                                                           |
| QUARTILES   | 四分位数   | SELECT PERCENTILE*CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1*{column}, PERCENTILE*CONT(0.5) WITHIN GROUP (ORDER BY {column}) as q2*{column}, PERCENTILE*CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3*{column} FROM {table} |
| PERCENTILES | 百分位数   | SELECT PERCENTILE*CONT(0.1) WITHIN GROUP (ORDER BY {column}) as p10*{column}, ... FROM {table}                                                                                                                                         |

### 1.2 文本字段分析

| 操作类型           | 说明        | SQL 模板示例                                                                                     |
| ------------------ | ----------- | ------------------------------------------------------------------------------------------------ |
| COUNT              | 计数        | SELECT COUNT({column}) as count\_{column} FROM {table}                                           |
| DISTINCT_COUNT     | 唯一值计数  | SELECT COUNT(DISTINCT {column}) as distinct*count*{column} FROM {table}                          |
| TOP_K              | 前 K 高频值 | SELECT {column}, COUNT(\*) as count FROM {table} GROUP BY {column} ORDER BY count DESC LIMIT {k} |
| VALUE_DISTRIBUTION | 值分布      | SELECT {column}, COUNT(\*) as count FROM {table} GROUP BY {column}                               |
| LENGTH_ANALYSIS    | 长度分析    | SELECT LENGTH({column}) as length, COUNT(\*) as count FROM {table} GROUP BY length               |
| PATTERN_ANALYSIS   | 模式分析    | SELECT {column}, COUNT(\*) as count FROM {table} GROUP BY {column}                               |

### 1.3 时间字段分析

| 操作类型          | 说明       | SQL 模板示例                                                                                   |
| ----------------- | ---------- | ---------------------------------------------------------------------------------------------- |
| COUNT             | 计数       | SELECT COUNT({column}) as count\_{column} FROM {table}                                         |
| DATE_RANGE        | 日期范围   | SELECT MIN({column}) as min_date, MAX({column}) as max_date FROM {table}                       |
| YEAR_ANALYSIS     | 年度分析   | SELECT EXTRACT(YEAR FROM {column}) as year, COUNT(\*) as count FROM {table} GROUP BY year      |
| MONTH_ANALYSIS    | 月度分析   | SELECT EXTRACT(MONTH FROM {column}) as month, COUNT(\*) as count FROM {table} GROUP BY month   |
| DAY_ANALYSIS      | 日分析     | SELECT EXTRACT(DAY FROM {column}) as day, COUNT(\*) as count FROM {table} GROUP BY day         |
| HOUR_ANALYSIS     | 小时分析   | SELECT EXTRACT(HOUR FROM {column}) as hour, COUNT(\*) as count FROM {table} GROUP BY hour      |
| WEEKDAY_ANALYSIS  | 星期分析   | SELECT EXTRACT(DOW FROM {column}) as weekday, COUNT(\*) as count FROM {table} GROUP BY weekday |
| SEASONAL_ANALYSIS | 季节性分析 | SELECT CASE ... as season, COUNT(\*) as count FROM {table} GROUP BY season                     |

### 1.4 通用分析

| 操作类型       | 说明         | SQL 模板示例                                                                                                                |
| -------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------- |
| COUNT          | 计数         | SELECT COUNT({column}) as count\_{column} FROM {table}                                                                      |
| MISSING_VALUES | 缺失值分析   | SELECT COUNT(_) as total_count, COUNT({column}) as non_null_count, COUNT(_) - COUNT({column}) as null_count FROM {table}    |
| DATA_QUALITY   | 数据质量检查 | SELECT COUNT(\*) as total_count, COUNT({column}) as non_null_count, COUNT(DISTINCT {column}) as distinct_count FROM {table} |
| CORRELATION    | 相关性分析   | SELECT CORR({column}, {column}) as correlation FROM {table}                                                                 |

> 注：部分分析类型如 COUNT、TOP_K 等可用于多种字段类型，具体可用性由后端类型判断逻辑决定。

## 2. SQL 生成规则

### 2.1 单字段分析

- 仅分析一个字段时，生成如：
  ```sql
  SELECT COUNT(Gender) as count_Gender FROM users;
  ```
- 支持别名：
  ```sql
  SELECT Gender as gender_alias FROM users;
  ```

### 2.2 分组分析（Group By）

- 有分组字段时：
  ```sql
  SELECT Gender, COUNT(*) as count FROM users GROUP BY Gender;
  ```
- 多字段分组：
  ```sql
  SELECT Gender, Age, COUNT(*) as count FROM users GROUP BY Gender, Age;
  ```

### 2.3 过滤条件（Where）

- 支持多条件：
  ```sql
  SELECT Gender, COUNT(*) as count FROM users WHERE Age > 18 AND Country = 'China' GROUP BY Gender;
  ```
- 过滤条件格式：
  - 等值：`column = value`
  - 范围：`column > value`、`column BETWEEN a AND b`
  - 字符串：`column LIKE '%xxx%'`

### 2.4 多字段分析

- 多字段多操作时，SELECT 子句拼接：
  ```sql
  SELECT Gender, COUNT(*) as count, AVG(Age) as avg_Age FROM users GROUP BY Gender;
  ```

### 2.5 Limit 与排序

- TOP_K、分布等分析默认带排序和 LIMIT：
  ```sql
  SELECT Gender, COUNT(*) as count FROM users GROUP BY Gender ORDER BY count DESC LIMIT 10;
  ```

### 2.6 排序（Sort By）

- 支持对结果按一个或多个字段排序：
  ```sql
  SELECT Gender, COUNT(*) as count FROM users GROUP BY Gender ORDER BY count DESC;
  SELECT Gender, AVG(Age) as avg_Age FROM users GROUP BY Gender ORDER BY avg_Age ASC, Gender DESC;
  ```
- 排序参数结构：
  - `sort_by`: 数组，每项为 `{ field: 字段名, order: 'ASC' | 'DESC' }`
  - 示例：
    ```json
    {
      "operations": [{ "field": "Age", "type": "AVG", "alias": "" }],
      "group_by": ["Gender"],
      "filters": [],
      "sort_by": [
        { "field": "avg_Age", "order": "DESC" },
        { "field": "Gender", "order": "ASC" }
      ],
      "task_id": "xxxx"
    }
    ```
- 生成 SQL：
  ```sql
  SELECT Gender, AVG(Age) as avg_Age FROM users GROUP BY Gender ORDER BY avg_Age DESC, Gender ASC;
  ```

## 3. 生成 SQL 的参数结构要求

- 字段名、表名需转义防注入（如`"Gender"`）。
- 别名可选，若无则自动生成（如`count_Gender`）。
- group_by、filters、limit 等参数需明确传递。
- 空数组需传`[]`，不可为 null。

## 4. 典型请求与 SQL 示例

### 4.1 仅 SELECT 字段

```json
{
  "operations": [{ "field": "Gender", "type": "SELECT", "alias": "" }],
  "group_by": [],
  "filters": [],
  "task_id": "xxxx"
}
```

生成 SQL：

```sql
SELECT Gender FROM users;
```

### 4.2 分组计数

```json
{
  "operations": [{ "field": "Gender", "type": "COUNT", "alias": "" }],
  "group_by": ["Gender"],
  "filters": [],
  "task_id": "xxxx"
}
```

生成 SQL：

```sql
SELECT Gender, COUNT(*) as count FROM users GROUP BY Gender;
```

### 4.3 带过滤条件

```json
{
  "operations": [{ "field": "Age", "type": "AVG", "alias": "" }],
  "group_by": ["Gender"],
  "filters": [{ "field": "Country", "op": "=", "value": "China" }],
  "task_id": "xxxx"
}
```

生成 SQL：

```sql
SELECT Gender, AVG(Age) as avg_Age FROM users WHERE Country = 'China' GROUP BY Gender;
```

## 5. 约定与注意事项

- 字段类型需与分析类型匹配（如 AVG 仅用于数值型）。
- 空值处理：部分分析需加`IS NOT NULL`判断。
- SQL 注入防护：所有字段、表名、值需转义。
- LIMIT、ORDER BY 等需按分析类型自动补充。
- 多字段分析时，SELECT、GROUP BY 需同步。
