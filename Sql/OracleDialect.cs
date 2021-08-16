using System;
using System.Collections.Generic;
using System.Text;

namespace DapperExtensions.Sql
{
    public class OracleDialect : SqlDialectBase
    {
        public OracleDialect() { }

        public override string GetIdentitySql(string tableName)
        {
            throw new System.NotImplementedException("Oracle does not support get last inserted identity.");
        }

        public override bool SupportsMultipleStatements
        {
            get { return false; }
        }

        //from Simple.Data.Oracle implementation https://github.com/flq/Simple.Data.Oracle/blob/master/Simple.Data.Oracle/OraclePager.cs
        public override string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters)
        {
            if (page > 0) page--;

            if (parameters == null) throw new ArgumentNullException(nameof(parameters));
            var toSkip = page * resultsPerPage;
            var topLimit = (page + 1) * resultsPerPage;
         
            var sb = new StringBuilder();
            sb.AppendLine("SELECT * FROM (");
            sb.AppendLine("SELECT ss_dapper_1_.*, ROWNUM RNUM FROM (");
            sb.Append(sql);
            sb.AppendLine(") ss_dapper_1_");
            sb.AppendLine("WHERE ROWNUM <= :topLimit) ss_dapper_2_ ");
            sb.AppendLine("WHERE ss_dapper_2_.RNUM > :toSkip");
            parameters.Add(":topLimit", topLimit);
            parameters.Add(":toSkip", toSkip);
            
            return sb.ToString();
        }

        public override string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));
            var sb = new StringBuilder();
            sb.AppendLine("SELECT * FROM (");
            sb.AppendLine("SELECT ss_dapper_1_.*, ROWNUM RNUM FROM (");
            sb.Append(sql);
            sb.AppendLine(") ss_dapper_1_");
            sb.AppendLine("WHERE ROWNUM <= :topLimit) ss_dapper_2_ ");
            sb.AppendLine("WHERE ss_dapper_2_.RNUM > :toSkip");
            
            parameters.Add(":topLimit", maxResults + firstResult);
            parameters.Add(":toSkip", firstResult);

            return sb.ToString();
        }

        public override string QuoteString(string value)
        {
            if (OpenQuote == '~') return value.Trim();
            if (value != null && value[0]=='`')
            {
                return string.Format("{0}{1}{2}", OpenQuote, value.Substring(1, value.Length - 2), CloseQuote);
            }
            if (value[0] != '"')
                return string.Format("{0}{1}{2}", OpenQuote, value, CloseQuote);
            else return value;
        }

        public override char ParameterPrefix
        {
            get { return ':'; }
        }
        public override char CloseQuote
        {
            get
            {
                return '~';
            }
        }
        public override char OpenQuote
        {
            get
            {
                return '~';
            }
        }
    }
}