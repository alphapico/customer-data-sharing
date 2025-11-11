using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;

namespace CustomerDataSharingLogic.Helpers
{
    public class BaseDBClass
    {
        public static void FillObjectByDataRow(DataRow row, Object obj)
        {
            //pass all properties of the business object and fill them with the values of that data row
            PropertyInfo[] properties = obj.GetType().GetProperties();
            foreach (PropertyInfo property in properties)
            {
                String rowName = property.Name.ToUpper();
                if (row.Table.Columns.Contains(rowName))
                {
                    if (row[rowName] == null || row[rowName] is DBNull)
                    {
                        property.SetValue(obj, null, null);
                        continue;
                    }

                    object newValue = null;
                    try
                    {
                        newValue = ParseProperty(property.PropertyType, row[rowName]);
                    }
                    catch
                    {
                        throw;
                    }

                    property.SetValue(obj, newValue, null);
                }
            }
        }

        internal static object ParseProperty(Type propertyType, object inputValue)
        {
            //if the value is null -> return null
            if (inputValue == null)
                return null;

            //get the type of the value
            Type valueType = inputValue.GetType();

            try
            {
                Type inputValueType = Nullable.GetUnderlyingType(valueType)
                    ?? valueType;

                if ((propertyType == typeof(Boolean) || propertyType == typeof(Boolean?)) &&
                    (inputValueType == typeof(Int16) || inputValueType == typeof(Int32) || inputValueType == typeof(Int64)))
                {
                    if (inputValue == null && propertyType == typeof(Boolean?))
                        return null;
                    else
                        return Convert.ToInt64(inputValue) != 0;
                }
                else
                {
                    bool isTargetNullable = propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>);

                    if (isTargetNullable)
                        propertyType = Nullable.GetUnderlyingType(propertyType);

                    if (propertyType == typeof(Double) && inputValue != null)
                    {
                        if (valueType == typeof(Double))
                        {
                            double d = (double)inputValue;
                            return d;
                        }
                        else if (valueType == typeof(String))
                        {
                            double d = Convert.ToDouble(Double.Parse(inputValue.ToString().Replace(",", ".")));
                            return d;
                        }
                    }

                    return (inputValue == null) ? null : Convert.ChangeType(inputValue, propertyType);
                }
            }
            catch
            {
                throw new Exception("The type of the class parameter (" + propertyType +
                                    ") doesn't conform to the type of the value (" + valueType + ")");
            }
        }
    }
}
