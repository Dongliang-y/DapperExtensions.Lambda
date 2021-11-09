using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using DapperExtensions.Mapper;

namespace DapperExtensions.Sql
{
    public interface ISqlGenerator
    {
        IDapperExtensionsConfiguration Configuration { get; }
        
        string Select(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters);
        string Select(IClassMapper classMap, IEnumerable<string> columNames, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters);
        string SelectPaged(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int page, int resultsPerPage, IDictionary<string, object> parameters);
        string SelectInnerJoin(IClassMapper classMap, Dictionary<string, IClassMapper> relationalClass, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters);
        string SelectJoin(IClassMapper classMap, Dictionary<string, IClassMapper> relationalClass,string joinWord,IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters);
      
        //string SelectPaged(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int page, int resultsPerPage, IDictionary<string, object> parameters);
        string SelectSet(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDictionary<string, object> parameters);
        string Count(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters);
        string Max(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters);
        string Min(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters);
        string AVG(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters);
        string Sum(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters);
        string Insert(IClassMapper classMap);

        /// <summary>
        /// 批量新增, mysql同时校验数据是否已存在，存在则更新，否则新增。
        /// </summary>
        string Inserts(IClassMapper classMap, int valueCount);
        string Update(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters, bool ignoreAllKeyProperties);
        string Delete(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters);

        string IdentitySql(IClassMapper classMap);
        string GetTableName(IClassMapper map);
        string GetColumnName(IClassMapper map, IPropertyMap property, bool includeAlias);
        string GetColumnName(IClassMapper map, string propertyName, bool includeAlias);
        bool SupportsMultipleStatements();
    }

    public class SqlGeneratorImpl : ISqlGenerator
    {
        public SqlGeneratorImpl(IDapperExtensionsConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IDapperExtensionsConfiguration Configuration { get; private set; }

        public virtual string Select(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters)
        {
            return Select(classMap, null, predicate, sort, parameters);
        }

        public virtual string Select(IClassMapper classMap,IEnumerable<string> columNames, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }


            var colums = "";
            if(columNames!=null&&columNames.Any())
            {
                colums = columNames.AppendStrings();
            }
            else
            {
                colums = BuildSelectColumns(classMap);
            }


            StringBuilder sql = new StringBuilder(string.Format("SELECT {0} FROM {1}",
                colums,
                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }

            if (sort != null && sort.Any())
            {
                sql.Append(" ORDER BY ")
                    .Append(sort.Select(s => GetColumnName(classMap, s.PropertyName, false) + (s.Ascending ? " ASC" : " DESC")).AppendStrings());
            }

            System.Console.WriteLine(sql.ToString());

            return sql.ToString();
        }

        public virtual string SelectPaged(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int page, int resultsPerPage, IDictionary<string, object> parameters)
        {
            if (sort == null || !sort.Any())
            {
                throw new ArgumentNullException("Sort", "Sort cannot be null or empty.");
            }

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }

            StringBuilder innerSql = new StringBuilder(string.Format("SELECT {0} FROM {1}",
                BuildSelectColumns(classMap),
                GetTableName(classMap)));
            if (predicate != null)
            {
                innerSql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            try
            {
                string orderBy =
                    sort.Select(s => GetColumnName(classMap, s.PropertyName, false) + (s.Ascending ? " ASC" : " DESC"))
                        .AppendStrings();
                innerSql.Append(" ORDER BY " + orderBy);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"sort 排序字段错误！{ex}");
            }
            string sql = Configuration.Dialect.GetPagingSql(innerSql.ToString(), page, resultsPerPage, parameters);

            System.Console.WriteLine(sql.ToString());

            return sql;
        }

        public virtual string SelectSet(IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {
            if (sort == null || !sort.Any())
            {
                throw new ArgumentNullException("Sort", "Sort cannot be null or empty.");
            }

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }

            StringBuilder innerSql = new StringBuilder(string.Format("SELECT {0} FROM {1}",
                BuildSelectColumns(classMap),
                GetTableName(classMap)));
            if (predicate != null)
            {
                innerSql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }

            string orderBy = sort.Select(s => GetColumnName(classMap, s.PropertyName, false) + (s.Ascending ? " ASC" : " DESC")).AppendStrings();
            innerSql.Append(" ORDER BY " + orderBy);

            string sql = Configuration.Dialect.GetSetSql(innerSql.ToString(), firstResult, maxResults, parameters);
            System.Console.WriteLine(sql.ToString());

            return sql;
        }


        public virtual string Count(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            string startChar="", endChar = "";
            if (Configuration.Dialect.OpenQuote != '~')
            {
                startChar = $"{ Configuration.Dialect.OpenQuote}";
                endChar = $"{ Configuration.Dialect.CloseQuote}";
            }
            StringBuilder sql = new StringBuilder(string.Format("SELECT COUNT(1) AS {0}TOTAL{1} FROM {2}",
                               startChar,
                                endChar,
                                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            System.Console.WriteLine(sql.ToString());
            return sql.ToString();
        }

        public string Max(IClassMapper classMap,string attrName, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }

            string startChar = "", endChar = "";

            if (Configuration.Dialect.OpenQuote != '~')
            {
                startChar = $"{ Configuration.Dialect.OpenQuote}";
                endChar = $"{ Configuration.Dialect.CloseQuote}";
            }
            StringBuilder sql = new StringBuilder(string.Format("SELECT MAX({0}) AS {1}MAXVAL{2} FROM {3}",
                                attrName,
                                startChar,
                                endChar,
                                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            System.Console.WriteLine(sql.ToString());
            return sql.ToString();
        }

        public string Min(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            string startChar = "", endChar = "";
            if (Configuration.Dialect.OpenQuote != '~')
            {
                startChar = $"{ Configuration.Dialect.OpenQuote}";
                endChar = $"{ Configuration.Dialect.CloseQuote}";
            }
            StringBuilder sql = new StringBuilder(string.Format("SELECT Min({0}) AS {1}MINVAL{2} FROM {3}",
                                attrName,
                               startChar,
                                endChar,
                                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            System.Console.WriteLine(sql.ToString());
            return sql.ToString();
        }

        public string AVG(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            string startChar = "", endChar = "";
            if (Configuration.Dialect.OpenQuote != '~')
            {
                startChar = $"{ Configuration.Dialect.OpenQuote}";
                endChar = $"{ Configuration.Dialect.CloseQuote}";
            }
            StringBuilder sql = new StringBuilder(string.Format("SELECT AVG({0}) AS {1}AVGVAL{2} FROM {3}",
                                attrName,
                               startChar,
                                endChar,
                                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            System.Console.WriteLine(sql.ToString());
            return sql.ToString();
        }

        public string Sum(IClassMapper classMap, string attrName, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            string startChar = "", endChar = "";
            if (Configuration.Dialect.OpenQuote != '~')
            {
                startChar = $"{ Configuration.Dialect.OpenQuote}";
                endChar = $"{ Configuration.Dialect.CloseQuote}";
            }
            StringBuilder sql = new StringBuilder(string.Format("SELECT SUM({0}) AS {1}SUMVAL{2} FROM {3}",
                                attrName,
                               startChar,
                                endChar,
                                GetTableName(classMap)));
            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }
            System.Console.WriteLine(sql.ToString());

            return sql.ToString();
        }

        public string Inserts(IClassMapper classMap, int valueCount)
        {
            var columns = classMap.Properties.Where(p => !(p.Ignored || p.IsReadOnly));
            if (!columns.Any())
            {
                throw new ArgumentException("No columns were mapped.");
            }

            var columnNames = columns.Select(p => GetColumnName(classMap, p, false));
            var parameters = columns.Select(p => Configuration.Dialect.ParameterPrefix + p.Name);

            string sql = string.Format("INSERT INTO {0} ({1}) VALUES ",
                                       GetTableName(classMap),
                                       columnNames.AppendStrings());
            var sqlRst = Configuration.Dialect.GetInsertsValue(sql, columns.Select(p =>p.Name), valueCount);
            if (sqlRst.Length > 1000000)
            {
                System.Console.WriteLine($"!!!!SQL语句长度达到{sqlRst.Length},超过警告值，请适当优化！");
              //  System.Diagnostics.Debug.WriteLine($"!!!!SQL语句长度达到{sqlRst.Length}有超出数据库限制的风险，请适当分配执行！！！");
            }
            else
            {
                System.Console.WriteLine(sql.ToString());
            }

            return sqlRst;
        }

        public virtual string Insert(IClassMapper classMap)
        {
            var columns = classMap.Properties.Where(p => !(p.Ignored || p.IsReadOnly || p.KeyType == KeyType.Identity || p.KeyType == KeyType.TriggerIdentity));
            if (!columns.Any())
            {
                throw new ArgumentException("No columns were mapped.");
            }

            var columnNames = columns.Select(p => GetColumnName(classMap, p, false));
            var parameters = columns.Select(p => Configuration.Dialect.ParameterPrefix + p.Name);

            string sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})",
                                       GetTableName(classMap),
                                       columnNames.AppendStrings(),
                                       parameters.AppendStrings());

            var triggerIdentityColumn = classMap.Properties.Where(p => p.KeyType == KeyType.TriggerIdentity).ToList();

            if (triggerIdentityColumn.Count > 0)
            {
                if (triggerIdentityColumn.Count > 1)
                    throw new ArgumentException("TriggerIdentity generator cannot be used with multi-column keys");

                sql += string.Format(" RETURNING {0} INTO {1}IdOutParam", triggerIdentityColumn.Select(p => GetColumnName(classMap, p, false)).First(), Configuration.Dialect.ParameterPrefix);
            }

           // System.Diagnostics.Debug.WriteLine(sql.ToString());
            System.Console.WriteLine(sql.ToString());

            return sql;
        }

        public virtual string Update(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters, bool ignoreAllKeyProperties)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException("Predicate");
            }

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            
            var columns = ignoreAllKeyProperties
                ? classMap.Properties.Where(p => !(p.Ignored || p.IsReadOnly) && p.KeyType == KeyType.NotAKey)
                : classMap.Properties.Where(p => !(p.Ignored || p.IsReadOnly || p.KeyType == KeyType.Identity || p.KeyType == KeyType.Assigned));

            if (!columns.Any())
            {
                throw new ArgumentException("No columns were mapped.");
            }

            var setSql =
                columns.Select(
                    p =>
                    string.Format(
                        "{0} = {1}{2}", GetColumnName(classMap, p, false), Configuration.Dialect.ParameterPrefix, p.Name));

            string sql= string.Format("UPDATE {0} SET {1} WHERE {2}",
                GetTableName(classMap),
                setSql.AppendStrings(),
                predicate.GetSql(this, parameters));

            System.Console.WriteLine(sql.ToString());

            return sql;
        }

        public virtual string Delete(IClassMapper classMap, IPredicate predicate, IDictionary<string, object> parameters)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException("Predicate");
            }

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }

            StringBuilder sql = new StringBuilder(string.Format("DELETE FROM {0}", GetTableName(classMap)));
            sql.Append(" WHERE ").Append(predicate.GetSql(this, parameters));

            System.Console.WriteLine(sql.ToString());

            return sql.ToString();
        }
        
        public virtual string IdentitySql(IClassMapper classMap)
        {
            return Configuration.Dialect.GetIdentitySql(GetTableName(classMap));
        }

        public virtual string GetTableName(IClassMapper map)
        {
            return Configuration.Dialect.GetTableName(map.SchemaName, map.TableName, null);
        }

        public virtual string GetColumnName(IClassMapper map, IPropertyMap property, bool includeAlias)
        {
            string alias = null;
            if (property.ColumnName != property.Name && includeAlias)
            {
                alias = property.Name;
            }

            return Configuration.Dialect.GetColumnName(GetTableName(map), property.ColumnName, alias);
        }

        public virtual string GetColumnName(IClassMapper map, string propertyName, bool includeAlias)
        {
            IPropertyMap propertyMap = map.Properties.SingleOrDefault(p => p.Name.Equals(propertyName, StringComparison.InvariantCultureIgnoreCase));
            if (propertyMap == null)
            {
                throw new ArgumentException(string.Format("Could not find '{0}' in Mapping.", propertyName));
            }

            return GetColumnName(map, propertyMap, includeAlias);
        }

        public virtual bool SupportsMultipleStatements()
        {
            return Configuration.Dialect.SupportsMultipleStatements;
        }

        public virtual string BuildSelectColumns(IClassMapper classMap)
        {
            var columns = classMap.Properties
                .Where(p => !p.Ignored)
                .Select(p => GetColumnName(classMap, p, true));
            return columns.AppendStrings();
        }

        public string SelectInnerJoin(IClassMapper classMap, Dictionary<string, IClassMapper> relationalClass, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters)
        {
            return SelectJoin(classMap, relationalClass, "inner join", predicate, sort, parameters);
        }

        public string SelectJoin(IClassMapper classMap, Dictionary<string, IClassMapper> relationalClass,string joinWord, IPredicate predicate, IList<ISort> sort, IDictionary<string, object> parameters)
        {

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }
            var manName = this.GetTableName(classMap);
            StringBuilder sql = new StringBuilder($"select * from {manName}");
            
            // 连接查询
            foreach (var map in relationalClass)
            {
                var childTabName = GetTableName(map.Value);
                var relColName = map.Key;
                sql.Append($" {joinWord} {childTabName} on {manName}.{relColName}={childTabName}.{map.Value.KeyName}");
             }

            if (predicate != null)
            {
                sql.Append(" WHERE ")
                    .Append(predicate.GetSql(this, parameters));
            }

            if (sort != null && sort.Any())
            {
                sql.Append(" ORDER BY ")
                    .Append(sort.Select(s => GetColumnName(classMap, s.PropertyName, false) + (s.Ascending ? " ASC" : " DESC")).AppendStrings());
            }

            System.Console.WriteLine(sql.ToString());

            return sql.ToString();
        }
    }
}