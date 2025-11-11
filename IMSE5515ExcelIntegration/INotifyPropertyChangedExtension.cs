using System;
using System.ComponentModel;
using System.Linq.Expressions;

namespace IMSE5515ExcelIntegration
{
    public static class INotifyPropertyChangedExtension
    {
        public static void NotifyPropertyChanged<T>(this INotifyPropertyChanged notify,
            PropertyChangedEventHandler handler,
            Expression<Func<T>> expression)
        {
            if (expression.Body is MemberExpression body && handler != null)
                handler(notify, new PropertyChangedEventArgs(body.Member.Name));
        }
    }
}