### 产品名称：DapperExtensions-y分支
### 公司：
### 作者：Thad Smith, Page Brooks
### 分支修改：Dongliang Yi
### 基本说明：

基于DapperExtensions 做了大量定制化开发,主要是为了更好的支持 拉姆达表达式，以及对.NET CORE 5的支持

### 更新记录：

2021-1-3 V3.0.0.17 修复分页关键字处理的一个BUG
2020-12-17 V3.0.0.16  ID自动生成，只补全ID
2020-9-24 V3.0.0.15 sql输出改成console输出
2020-9-24 V3.0.0.13 分页BUG修复。
2020-9-24 V3.0.0.12 接口二义性问题
2020-9-24 V3.0.0.9 增加自定义列名查询数据。
2020-9-21 v3.0.0.8
增加 max、min、sum、avg 数学函数。
2020-9-19 v3.0.0.6
增加把 表达式转cheng sql的扩展方法。
添加批量新增时sql超长风险提示。
2020-7-19:
修复一个ID生成的BUG
2020-7-17:
重写批量新增，使用一条sql，多个新增值得方式。mysql支持addOrUpdate
2020-7-10 : 
优化分页查询，升级.net core 3.1
2020-4-29
原作者：Thad Smith, Page Brooks
回滚了对Contains的支持。 调试模式下将提示性能问题，请自行负责优化。
2020-06-02
重命名ID生成类，避免和底层的冲突。

2019-7-XX： 主要扩展了对拉姆达表达式解析的支持，增加 QueryBuilder.cs

##示例：
```
 /// <summary>
        /// 筛选数据
        /// </summary>
        /// <param name="filter">筛选条件</param>
        /// <param name="sorts">排序字段</param>
        /// <returns>实体集合</returns>
        public virtual IEnumerable<TEntity> GetListByExp(
            Expression<Func<TEntity, bool>> filter,
            IList<Sort> sorts = null)
        {
            // 应用过滤器
            foreach (var ft in this.Options.QueryFilters)
            {
                var exp = ft.Value.GetFilter<TEntity>();
                if (exp != null)
                {
                    filter = filter.AndAlso(exp);
                }
            }
            var predicate = QueryBuilder<TEntity>.FromExpression(filter);
            IList<ISort> dapperSorts = null;
            if (sorts != null)
            {
                dapperSorts = sorts.Select(t => new Sort { Ascending = t.Ascending, PropertyName = t.PropertyName })
                    .ToArray();
            }

            using (var rsp = this.CreateDbContent())
            {
                var result= rsp.GetList<TEntity>(predicate, dapperSorts).ToList();
                return result;
            }
        }

        /// <summary>
        /// 指定字段范围查询，返回的实体只有这几个字段有值，目的是为了避免字段多时全字段查询（select *）
        /// </summary>
        /// <param name="columnNames">需要指定查询的字段</param>
        /// <param name="filter">筛选条件</param>
        /// <param name="sorts">排序字段</param>
        /// <returns>实体集合</returns>
        public virtual IEnumerable<TEntity> GetListWithColumns(IEnumerable<string> columnNames,
            Expression<Func<TEntity, bool>> filter,
            IList<Sort> sorts = null)
        {
            // 应用过滤器
            foreach (var ft in this.Options.QueryFilters)
            {
                var exp = ft.Value.GetFilter<TEntity>();
                if (exp != null)
                {
                    filter = filter.AndAlso(exp);
                }
            }
            var predicate = QueryBuilder<TEntity>.FromExpression(filter);
            IList<ISort> dapperSorts = null;
            if (sorts != null)
            {
                dapperSorts = sorts.Select(t => new Sort { Ascending = t.Ascending, PropertyName = t.PropertyName })
                    .ToArray();
            }

            using (var rsp = this.CreateDbContent())
            {
                var result = rsp.GetListWithColumns<TEntity>(columnNames,predicate, dapperSorts).ToList();
                return result;
            }
        }

        /// <summary>
        /// 分页查询对象集合,起始页码0
        /// </summary>
        /// <param name="pageIndex">页码</param>
        /// <param name="pageSize">页大小</param>
        /// <param name="filter">数据筛选</param>
        /// <param name="sorts">排序字段</param>
        /// <returns>实体集合</returns>
        public virtual IListResult<TEntity> GetPaged(
            int pageIndex,
            int pageSize,
            Expression<Func<TEntity, bool>> filter,
            IList<Sort> sorts)
        {
            // 应用过滤器
            foreach (var ft in this.Options.QueryFilters)
            {
                var exp = ft.Value.GetFilter<TEntity>();
                if (exp != null)
                {
                    filter = filter.AndAlso(ft.Value.GetFilter<TEntity>());
                }
            }

            var predicate = QueryBuilder<TEntity>.FromExpression(filter);
            IList<ISort> dapperSorts = null;
            if (sorts != null)
            {
                dapperSorts = sorts.Select(t => new Sort { Ascending = t.Ascending, PropertyName = t.PropertyName })
                    .ToArray();
            }
            else
            {
                dapperSorts = new List<ISort> { new Sort { Ascending = false, PropertyName = "Id" } };
            }

            // var listPage = this.DBContext.GetPage<TEntity>(predicate, dapperSorts, pageIndex, pageSize);
            // dataCount = DBContext.Count<TEntity>(predicate);
            using (var rsp = this.CreateDbContent())
            {
                var listPage = rsp.GetPage<TEntity>(predicate, dapperSorts, pageIndex, pageSize);
                return new ListResult<TEntity>(listPage.Item1, listPage.Item2);
            }
        }
```