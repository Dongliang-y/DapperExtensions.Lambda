using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using DapperExtensions.Sql;
using DapperExtensions.Mapper;
using System.Threading.Tasks;

namespace DapperExtensions
{
    public static class DapperExtensions
    {
        private readonly static object _lock = new object();

        private static Func<IDapperExtensionsConfiguration, IDapperImplementor> _instanceFactory;
        private static IDapperImplementor _instance;
        private static IDapperExtensionsConfiguration _configuration;
        
        /// <summary>
        /// Gets or sets the default class mapper to use when generating class maps. If not specified, AutoClassMapper<T> is used.
        /// DapperExtensions.Configure(Type, IList<Assembly>, ISqlDialect) can be used instead to set all values at once
        /// </summary>
        public static Type DefaultMapper
        {
            get
            {
                return _configuration.DefaultMapper;
            }

            set
            {
                Configure(value, _configuration.MappingAssemblies, _configuration.Dialect);
            }
        }

        /// <summary>
        /// Gets or sets the type of sql to be generated.
        /// DapperExtensions.Configure(Type, IList<Assembly>, ISqlDialect) can be used instead to set all values at once
        /// </summary>
        public static ISqlDialect SqlDialect
        {
            get
            {
                return _configuration.Dialect;
            }

            set
            {
                Configure(_configuration.DefaultMapper, _configuration.MappingAssemblies, value);
            }
        }
        
        /// <summary>
        /// Get or sets the Dapper Extensions Implementation Factory.
        /// </summary>
        public static Func<IDapperExtensionsConfiguration, IDapperImplementor> InstanceFactory
        {
            get
            {
                if (_instanceFactory == null)
                {
                    _instanceFactory = config => new DapperImplementor(new SqlGeneratorImpl(config));
                }

                return _instanceFactory;
            }
            set
            {
                _instanceFactory = value;
                Configure(_configuration.DefaultMapper, _configuration.MappingAssemblies, _configuration.Dialect);
            }
        }

        /// <summary>
        /// Gets the Dapper Extensions Implementation
        /// </summary>
        private static IDapperImplementor Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (_lock)
                    {
                        if (_instance == null)
                        {
                            _instance = InstanceFactory(_configuration);
                        }
                    }
                }

                return _instance;
            }
        }

        static DapperExtensions()
        {
            Configure(typeof(AutoClassMapper<>), new List<Assembly>(), new SqlServerDialect());
        }

        /// <summary>
        /// Add other assemblies that Dapper Extensions will search if a mapping is not found in the same assembly of the POCO.
        /// </summary>
        /// <param name="assemblies"></param>
        public static void SetMappingAssemblies(IList<Assembly> assemblies)
        {
            Configure(_configuration.DefaultMapper, assemblies, _configuration.Dialect);
        }

        /// <summary>
        /// Configure DapperExtensions extension methods.
        /// </summary>
        /// <param name="defaultMapper"></param>
        /// <param name="mappingAssemblies"></param>
        /// <param name="sqlDialect"></param>
        public static void Configure(IDapperExtensionsConfiguration configuration)
        {
            _instance = null;
            _configuration = configuration;
        }

        /// <summary>
        /// Configure DapperExtensions extension methods.
        /// </summary>
        /// <param name="defaultMapper"></param>
        /// <param name="mappingAssemblies"></param>
        /// <param name="sqlDialect"></param>
        public static void Configure(Type defaultMapper, IList<Assembly> mappingAssemblies, ISqlDialect sqlDialect)
        {
            Configure(new DapperExtensionsConfiguration(defaultMapper, mappingAssemblies, sqlDialect));
        }

        #region 增删改
        /// <summary>
        /// Executes an insert query for the specified entity.
        /// </summary>
        public static int Inserts<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Instance.Insert<T>(connection, entities, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes an insert query for the specified entity, returning the primary key.  
        /// If the entity has a single key, just the value is returned.  
        /// If the entity has a composite key, an IDictionary&lt;string, object&gt; is returned with the key values.
        /// The key value for the entity will also be updated if the KeyType is a Guid or Identity.
        /// </summary>
        public static dynamic Insert<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Instance.Insert<T>(connection, entity, transaction, commandTimeout);
        }


        /// <summary>
        /// Executes an insert query for the specified entity.
        /// </summary>
        public async static Task<int> InsertsAsync<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.InsertAsync<T>(connection, entities, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes an insert query for the specified entity, returning the primary key.  
        /// If the entity has a single key, just the value is returned.  
        /// If the entity has a composite key, an IDictionary&lt;string, object&gt; is returned with the key values.
        /// The key value for the entity will also be updated if the KeyType is a Guid or Identity.
        /// </summary>
        public static async Task<dynamic> InsertAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.InsertAsync<T>(connection, entity, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes an update query for the specified entity.
        /// </summary>
        public static bool Update<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null, bool ignoreAllKeyProperties = false) where T : class
        {
            return Instance.Update<T>(connection, entity, transaction, commandTimeout, ignoreAllKeyProperties);
        }

        /// <summary>
        /// Executes an update query for the specified entity.
        /// </summary>
        public static async Task<bool> UpdateAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null, bool ignoreAllKeyProperties = false) where T : class
        {
            return await Instance.UpdateAsync<T>(connection, entity, transaction, commandTimeout, ignoreAllKeyProperties);
        }

        /// <summary>
        /// Executes a delete query for the specified entity.
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Instance.Delete<T>(connection, entity, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes a delete query using the specified predicate.
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Instance.Delete<T>(connection, predicate, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes a delete query for the specified entity.
        /// </summary>
        public static async Task<int> DeleteAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.DeleteAsync<T>(connection, entity, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes a delete query using the specified predicate.
        /// </summary>
        public static async Task<int> DeleteAsync<T>(this IDbConnection connection, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.DeleteAsync<T>(connection, predicate, transaction, commandTimeout);
        }

        #endregion

        #region Get

        /// <summary>
        /// Executes a query for the specified id, returning the data typed as per T
        /// </summary>
        public static T Get<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var result = Instance.Get<T>(connection, id, transaction, commandTimeout);
            return (T)result;
        }

        /// <summary>
        /// Executes a query for the specified id, returning the data typed as per T
        /// </summary>
        public static async Task<T> GetAsync<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var result =await Instance.GetAsync<T>(connection, id, transaction, commandTimeout);
            return result;
        }

        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// </summary>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false) where T : class
        {
            return Instance.GetList<T>(connection, predicate, sort, transaction, commandTimeout, buffered);
        }
        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// </summary>
        public async static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.GetListAsync<T>(connection, predicate, sort, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// </summary>
        public static IEnumerable<T> GetListWithColumns<T>(this IDbConnection connection, IEnumerable<string> columnNames = null, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false) where T : class
        {
            return Instance.GetList<T>(connection, columnNames, predicate, sort, transaction, commandTimeout, buffered);
        }

        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// </summary>
        public static async Task<IEnumerable<T>> GetListWithColumnsAsync<T>(this IDbConnection connection, IEnumerable<string> columnNames = null, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false) where T : class
        {
            return await Instance.GetListAsync<T>(connection, columnNames, predicate, sort, transaction, commandTimeout, buffered);
        }

        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// Data returned is dependent upon the specified page and resultsPerPage.
        /// </summary>
        public async static Task<(IEnumerable<T>, long)> GetPageAsync<T>(this IDbConnection connection, object predicate, IList<ISort> sort, int page, int resultsPerPage, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.GetPageAsync<T>(connection, predicate, sort, page, resultsPerPage, transaction, commandTimeout);
        }
        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// Data returned is dependent upon the specified page and resultsPerPage.
        /// </summary>
        public static (IEnumerable<T>, long) GetPage<T>(this IDbConnection connection, object predicate, IList<ISort> sort, int page, int resultsPerPage, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false) where T : class
        {
            return Instance.GetPage<T>(connection, predicate, sort, page, resultsPerPage, transaction, commandTimeout, buffered);
        }

        /// <summary>
        /// Executes a select query using the specified predicate, returning an IEnumerable data typed as per T.
        /// Data returned is dependent upon the specified firstResult and maxResults.
        /// </summary>
        public static IEnumerable<T> GetSet<T>(this IDbConnection connection, object predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false) where T : class
        {
            return Instance.GetSet<T>(connection, predicate, sort, firstResult, maxResults, transaction, commandTimeout, buffered);
        }
        /// <summary>
        /// 多表关联查询，此查询会扫描Tmain的外键属性ForeignKey，执行关联查询，查询并映射关联属性。
        /// </summary>
        /// <typeparam name="TMain">主表类型</typeparam>
        /// <typeparam name="TRel1">外键对象1</typeparam>
        /// <typeparam name="TRel2">外键对象2</typeparam>
        /// <typeparam name="TRel3">外键对象3</typeparam>
        /// <typeparam name="TRel4">外键对象4</typeparam>
        /// <typeparam name="TRel5">外键对象5</typeparam>
        /// <param name="connection">数据库连接</param>
        /// <param name="func">用于主表和子表对象组合的委托</param>
        /// <param name="predicate">主表条件</param>
        /// <param name="sort">主表排序</param>
        /// <param name="transaction">事务</param>
        /// <param name="buffered">是否使用缓存</param>
        /// <param name="commandTimeout">连接超时时间</param>
        /// <returns>IEnumerable`TMain</returns>
        public static async Task<IEnumerable<TMain>> QueryRelationalAsync<TMain, TRel1, TRel2, TRel3, TRel4, TRel5>(IDbConnection connection, Func<TMain, TRel1, TRel2, TRel3, TRel4, TRel5, TMain> func, object predicate, IList<ISort> sort, IDbTransaction transaction=null, bool buffered=true, int? commandTimeout=180) where TMain : class
        {
            return await Instance.QueryRelationalAsync(connection, func, predicate, sort, transaction, buffered, commandTimeout);
        }
        public static async Task<IEnumerable<TMain>> QueryRelationalAsync<TMain, TRel1, TRel2, TRel3, TRel4>(IDbConnection connection, Func<TMain, TRel1, TRel2, TRel3, TRel4, TMain> func, object predicate, IList<ISort> sort, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = 180) where TMain : class
        {
            return await Instance.QueryRelationalAsync(connection, func, predicate, sort, transaction, buffered, commandTimeout);
        }
        public static async Task<IEnumerable<TMain>> QueryRelationalAsync<TMain, TRel1, TRel2, TRel3>(IDbConnection connection, Func<TMain, TRel1, TRel2, TRel3, TMain> func, object predicate, IList<ISort> sort, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = 180) where TMain : class
        {
            return await Instance.QueryRelationalAsync(connection, func, predicate, sort, transaction, buffered, commandTimeout);
        }
        public static async Task<IEnumerable<TMain>> QueryRelationalAsync<TMain, TRel1, TRel2>(IDbConnection connection, Func<TMain, TRel1, TRel2, TMain> func, object predicate, IList<ISort> sort, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = 180) where TMain : class
        {
            return await Instance.QueryRelationalAsync(connection, func, predicate, sort, transaction, buffered, commandTimeout);
        }
        public static async Task<IEnumerable<TMain>> QueryRelationalAsync<TMain, TRel1>(IDbConnection connection, Func<TMain, TRel1, TMain> func, object predicate, IList<ISort> sort, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = 180) where TMain : class
        {
            return await Instance.QueryRelationalAsync(connection, func, predicate, sort, transaction, buffered, commandTimeout);
        }

        #endregion

        #region count max min sum avg

        /// <summary>
        /// Executes a query using the specified predicate, returning an integer that represents the number of rows that match the query.
        /// </summary>
        public static int Count<T>(this IDbConnection connection, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Instance.Count<T>(connection, predicate, transaction, commandTimeout);
        }

        /// <summary>
        /// 求某列最大值
        /// </summary>
        public static TValue Max<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return Instance.Max<TValue,TEntity>(connection, attrName, transaction, predicate,  commandTimeout);
        }

        /// <summary>
        ///  求某列最小值
        /// </summary>
        public static TValue Min<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return Instance.Min<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }

        /// <summary>
        /// 求某列和
        /// </summary>
        public static TValue Sum<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return Instance.Sum<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }

        /// <summary>
        /// 求某列平均值。
        /// </summary>
        public static TValue AVG<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return Instance.AVG<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }


        /// <summary>
        /// 求某列最大值
        /// </summary>
        public static async Task<TValue> MaxAsync<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return await Instance.MaxAsync<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }

        /// <summary>
        /// Executes a query using the specified predicate, returning an integer that represents the number of rows that match the query.
        /// </summary>
        public async static Task<int> CountAsync<T>(this IDbConnection connection, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return await Instance.CountAsync<T>(connection, predicate, transaction, commandTimeout);
        }


        /// <summary>
        ///  求某列最小值
        /// </summary>
        public static async Task<TValue> MinAsync<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return await Instance.MinAsync<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }

        /// <summary>
        /// 求某列和
        /// </summary>
        public static async Task<TValue> SumAsync<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return await Instance.SumAsync<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }

        /// <summary>
        /// 求某列平均值。
        /// </summary>
        public static async Task<TValue> AVGAsync<TValue, TEntity>(this IDbConnection connection, string attrName, object predicate, IDbTransaction transaction = null, int? commandTimeout = null) where TEntity : class
        {
            return await Instance.AVGAsync<TValue, TEntity>(connection, attrName, transaction, predicate, commandTimeout);
        }
        #endregion

        /// <summary>
        /// Executes a select query for multiple objects, returning IMultipleResultReader for each predicate.
        /// </summary>
        public static IMultipleResultReader GetMultiple(this IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return Instance.GetMultiple(connection, predicate, transaction, commandTimeout);
        }

        /// <summary>
        /// Gets the appropriate mapper for the specified type T. 
        /// If the mapper for the type is not yet created, a new mapper is generated from the mapper type specifed by DefaultMapper.
        /// </summary>
        public static IClassMapper GetMap<T>() where T : class
        {
            return Instance.SqlGenerator.Configuration.GetMap<T>();
        }

        /// <summary>
        /// Clears the ClassMappers for each type.
        /// </summary>
        public static void ClearCache()
        {
            Instance.SqlGenerator.Configuration.ClearCache();
        }

        /// <summary>
        /// Generates a COMB Guid which solves the fragmented index issue.
        /// See: http://davybrion.com/blog/2009/05/using-the-guidcomb-identifier-strategy
        /// </summary>
        public static string GetNextID()
        {
            return Instance.SqlGenerator.Configuration.GetNextID().ToString();
        }
    }
}
