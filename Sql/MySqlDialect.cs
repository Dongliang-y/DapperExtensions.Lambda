using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DapperExtensions.Sql
{
    public class MySqlDialect : SqlDialectBase
    {
        public override char OpenQuote
        {
            get { return '`'; }
        }

        public override char CloseQuote
        {
            get { return '`'; }
        }

        public override string GetIdentitySql(string tableName)
        {
            return "SELECT CONVERT(LAST_INSERT_ID(), SIGNED INTEGER) AS ID";
        }

        public override string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters)
        {
            if (page > 0) page--;
            int startValue = page * resultsPerPage;
            var rstSql= GetSetSql(sql, startValue, resultsPerPage, parameters);
            return $"{rstSql};SELECT FOUND_ROWS();";
        }

        public override string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {
            sql = sql.ToLower();
            if (sql.StartsWith("select"))
            {
                sql = "select sql_calc_found_rows " + sql.Substring(6);
            }

            string result = string.Format("{0} LIMIT @firstResult, @maxResults ", sql);
            System.Diagnostics.Debug.WriteLine(result);
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));
            parameters.Add("@firstResult", firstResult);
            parameters.Add("@maxResults", maxResults);
            return result;
        }

        public override string GetInsertsValue(string sql, IEnumerable<string> columnNames, int entitysCount)
        {
            var sqlBase= base.GetInsertsValue(sql, columnNames, entitysCount);
            sqlBase += " ON DUPLICATE KEY UPDATE ";
            StringBuilder strUpdateColums = new StringBuilder();
            foreach (var col in columnNames)
            {
                strUpdateColums.Append($"{col}=VALUES({col}),");
            }
            sqlBase += strUpdateColums.ToString().Substring(0, strUpdateColums.Length - 1);
            return sqlBase;
        }
    }
}