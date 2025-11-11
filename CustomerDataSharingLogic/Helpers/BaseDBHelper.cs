using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace CustomerDataSharingLogic.Helpers
{
    public static class BaseDBHelper
    {
        public static List<T> GetList<T>(DataTable dtsData)
        {
            var dataList = new List<T>();
            foreach (DataRow row in dtsData.Rows)
            {
                var deliveryData = (T)Activator.CreateInstance(typeof(T));
                BaseDBClass.FillObjectByDataRow(row, deliveryData);
                dataList.Add(deliveryData);
            }
            return dataList;
        }
    }
}
