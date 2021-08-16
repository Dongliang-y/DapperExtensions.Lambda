using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace EntityFramework4.Logic
{
    /// <summary>
    /// 统一ParameterExpression
    /// </summary>
    internal class ParameterReplacer : ExpressionVisitor
    {
        public ParameterReplacer(ParameterExpression paramExpr)
        {
            ParameterExpression = paramExpr;
        }
        /// <summary>
        /// 在给定左操作数、右操作数、实现方法和类型转换函数的情况下，通过调用适当的工厂方法来创建一个 <see cref="T:System.Linq.Expressions.BinaryExpression"/>。
        /// </summary>
        /// 
        /// <returns>
        /// 通过调用适当的工厂方法生成的 <see cref="T:System.Linq.Expressions.BinaryExpression"/>。
        /// </returns>
        /// <param name="binaryType">指定二元运算类型的 <see cref="T:System.Linq.Expressions.ExpressionType"/>。</param>
        /// <param name="left">一个表示左操作数的 <see cref="T:System.Linq.Expressions.Expression"/>。</param>
        /// <param name="right">一个表示右操作数的 <see cref="T:System.Linq.Expressions.Expression"/>。</param>
        /// <param name="liftToNull">若要将 true 设置为 <see cref="P:System.Linq.Expressions.BinaryExpression.IsLiftedToNull"/>，则为 true；若要将 false 设置为 <see cref="P:System.Linq.Expressions.BinaryExpression.IsLiftedToNull"/>，则为 false。</param>
        /// <param name="method">一个指定实现方法的 <see cref="T:System.Reflection.MethodInfo"/>。</param>
        /// <param name="conversion">一个表示类型转换函数的 <see cref="T:System.Linq.Expressions.LambdaExpression"/>。只有在 <paramref name="binaryType"/> 为 <see cref="F:System.Linq.Expressions.ExpressionType.Coalesce"/> 或复合赋值时，才使用此参数。</param>
        /// <exception cref="T:System.ArgumentException"><paramref name="binaryType"/> 与二元表达式节点不对应。</exception><exception cref="T:System.ArgumentNullException"><paramref name="left"/> 或 <paramref name="right"/> 为 null。</exception>
        public static BinaryExpression MakeBinary(ExpressionType binaryType, Expression left, Expression right, bool liftToNull)
        {
            switch (binaryType)
            {
                case ExpressionType.Add:
                    return Expression.Add(left, right, method);
                case ExpressionType.AddChecked:
                    return Expression.AddChecked(left, right, method);
                case ExpressionType.And:
                    return Expression.And(left, right, method);
                case ExpressionType.AndAlso:
                    return Expression.AndAlso(left, right, method);
                case ExpressionType.ArrayIndex:
                    return Expression.ArrayIndex(left, right);
                case ExpressionType.Coalesce:
                    return Expression.Coalesce(left, right, conversion);
                case ExpressionType.Divide:
                    return Expression.Divide(left, right, method);
                case ExpressionType.Equal:
                    return Expression.Equal(left, right, liftToNull, method);
                case ExpressionType.ExclusiveOr:
                    return Expression.ExclusiveOr(left, right, method);
                case ExpressionType.GreaterThan:
                    return Expression.GreaterThan(left, right, liftToNull, method);
                case ExpressionType.GreaterThanOrEqual:
                    return Expression.GreaterThanOrEqual(left, right, liftToNull, method);
                case ExpressionType.LeftShift:
                    return Expression.LeftShift(left, right, method);
                case ExpressionType.LessThan:
                    return Expression.LessThan(left, right, liftToNull, method);
                case ExpressionType.LessThanOrEqual:
                    return Expression.LessThanOrEqual(left, right, liftToNull, method);
                case ExpressionType.Modulo:
                    return Expression.Modulo(left, right, method);
                case ExpressionType.Multiply:
                    return Expression.Multiply(left, right, method);
                case ExpressionType.MultiplyChecked:
                    return Expression.MultiplyChecked(left, right, method);
                case ExpressionType.NotEqual:
                    return Expression.NotEqual(left, right, liftToNull, method);
                case ExpressionType.Or:
                    return Expression.Or(left, right, method);
                case ExpressionType.OrElse:
                    return Expression.OrElse(left, right, method);
                case ExpressionType.Power:
                    return Expression.Power(left, right, method);
                case ExpressionType.RightShift:
                    return Expression.RightShift(left, right, method);
                case ExpressionType.Subtract:
                    return Expression.Subtract(left, right, method);
                case ExpressionType.SubtractChecked:
                    return Expression.SubtractChecked(left, right, method);
                case ExpressionType.Assign:
                    return Expression.Assign(left, right);
                case ExpressionType.AddAssign:
                    return Expression.AddAssign(left, right, method, conversion);
                case ExpressionType.AndAssign:
                    return Expression.AndAssign(left, right, method, conversion);
                case ExpressionType.DivideAssign:
                    return Expression.DivideAssign(left, right, method, conversion);
                case ExpressionType.ExclusiveOrAssign:
                    return Expression.ExclusiveOrAssign(left, right, method, conversion);
                case ExpressionType.LeftShiftAssign:
                    return Expression.LeftShiftAssign(left, right, method, conversion);
                case ExpressionType.ModuloAssign:
                    return Expression.ModuloAssign(left, right, method, conversion);
                case ExpressionType.MultiplyAssign:
                    return Expression.MultiplyAssign(left, right, method, conversion);
                case ExpressionType.OrAssign:
                    return Expression.OrAssign(left, right, method, conversion);
                case ExpressionType.PowerAssign:
                    return Expression.PowerAssign(left, right, method, conversion);
                case ExpressionType.RightShiftAssign:
                    return Expression.RightShiftAssign(left, right, method, conversion);
                case ExpressionType.SubtractAssign:
                    return Expression.SubtractAssign(left, right, method, conversion);
                case ExpressionType.AddAssignChecked:
                    return Expression.AddAssignChecked(left, right, method, conversion);
                case ExpressionType.MultiplyAssignChecked:
                    return Expression.MultiplyAssignChecked(left, right, method, conversion);
                case ExpressionType.SubtractAssignChecked:
                    return Expression.SubtractAssignChecked(left, right, method, conversion);
                default:
                    throw Error.UnhandledBinary((object)binaryType);
            }
        }

        public ParameterExpression ParameterExpression { get; private set; }

        public Expression Replace(Expression expr)
        {
            return Visit(expr);
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            return ParameterExpression;
        }
    }
    /// <summary>
    /// Predicate扩展
    /// </summary>
    public static class PredicateExtensionses
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Expression<Func<T, bool>> True<T>() { return f => true; }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Expression<Func<T, bool>> False<T>() { return f => false; }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expLeft"></param>
        /// <param name="expRight"></param>
        /// <returns></returns>
        public static Expression<Func<T, bool>> And<T>(this Expression<Func<T, bool>> expLeft, Expression<Func<T, bool>> expRight)
        {
            var candidateExpr = Expression.Parameter(typeof(T), "candidate");
            var parameterReplacer = new ParameterReplacer(candidateExpr);

            var left = parameterReplacer.Replace(expLeft.Body);
            var right = parameterReplacer.Replace(expRight.Body);
            var body = Expression.And(left, right);

            return Expression.Lambda<Func<T, bool>>(body, candidateExpr);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expLeft"></param>
        /// <param name="expRight"></param>
        /// <returns></returns>
        public static Expression<Func<T, bool>> Or<T>(this Expression<Func<T, bool>> expLeft, Expression<Func<T, bool>> expRight)
        {
            var candidateExpr = Expression.Parameter(typeof(T), "candidate");
            var parameterReplacer = new ParameterReplacer(candidateExpr);

            var left = parameterReplacer.Replace(expLeft.Body);
            var right = parameterReplacer.Replace(expRight.Body);
            var body = Expression.Or(left, right);

            return Expression.Lambda<Func<T, bool>>(body, candidateExpr);
        }
    }
    /// <summary>
    /// Queryable扩展
    /// </summary>
    public static class QueryableExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queryable"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static IQueryable<T> OrderBy<T>(this IQueryable<T> queryable, string propertyName)
        {
            return OrderBy(queryable, propertyName, false);

        }


        /// <summary>
        /// OrderBy
        /// </summary>
        /// <typeparam name="T">实体</typeparam>
        /// <param name="queryable">条件</param>
        /// <param name="propertyName">属性名称</param>
        /// <param name="desc">是否降序</param>
        /// <returns></returns>
        public static IQueryable<T> OrderBy<T>(this IQueryable<T> queryable, string propertyName, bool desc)
        {
            var param = Expression.Parameter(typeof(T));
            var body = Expression.Property(param, propertyName);
            dynamic keySelector = Expression.Lambda(body, param);

            return desc ? Queryable.OrderByDescending(queryable, keySelector) : Queryable.OrderBy(queryable, keySelector);
        }
    }


    public class CommonEqualityComparer<T, V> : IEqualityComparer<T>
    {
        private Func<T, V> keySelector;
        private IEqualityComparer<V> comparer;

        public CommonEqualityComparer(Func<T, V> keySelector, IEqualityComparer<V> comparer)
        {
            this.keySelector = keySelector;
            this.comparer = comparer;
        }

        public CommonEqualityComparer(Func<T, V> keySelector)
            : this(keySelector, EqualityComparer<V>.Default)
        { }

        public bool Equals(T x, T y)
        {
            return comparer.Equals(keySelector(x), keySelector(y));
        }

        public int GetHashCode(T obj)
        {
            return comparer.GetHashCode(keySelector(obj));
        }
    }
    public static class CommonFunction
    {
        /// <summary>
        /// 扩展Distinct方法
        /// </summary>
        /// <typeparam name=\"T\">源类型</typeparam>
        /// <typeparam name=\"V\">委托返回类型（根据V类型，排除重复项）</typeparam>
        /// <param name=\"source\">扩展源</param>
        /// <param name=\"keySelector\">委托（执行操作）</param>
        /// <returns></returns>
        public static IEnumerable<T> Distinct<T, V>(this IEnumerable<T> source, Func<T, V> keySelector)
        {
            return source.Distinct(new CommonEqualityComparer<T, V>(keySelector));
        }
    }
}

