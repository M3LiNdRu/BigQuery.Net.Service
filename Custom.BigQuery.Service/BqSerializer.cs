using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Custom.BigQuery.Service
{
    public class BigQueryDataSerializer
    {

        public T Deserialize<T>(TableRow row, TableSchema schema) where T : new()
        {
            T obj = new T();
            var props = typeof(T).GetProperties();

            using(var e1 = schema.Fields.GetEnumerator())
            using(var e2 = row.F.GetEnumerator())
            {
                while(e1.MoveNext() && e2.MoveNext())
                {
                    string name  = e1.Current.Name;
                    string type = e1.Current.Type;
                    string value = (string)e2.Current.V;

                    var prop = props.Where(x => x.Name.Equals(name)).SingleOrDefault();
                    if (prop != null) prop.SetValue(obj, Parse(type, value));
                }
            }

            return obj;
        }

        public TableDataInsertAllRequest.RowsData Serialize<T>(T obj) {

            TableDataInsertAllRequest.RowsData row = new TableDataInsertAllRequest.RowsData()
            {
                InsertId = Guid.NewGuid().ToString(),
                Json = AsDictionary(obj)
            };

            return row;
        }
        public TableSchema GetSchema(Type type)
        {
            TableSchema schema = new TableSchema()
            {
                Fields = new List<TableFieldSchema>()
            };

            var props = type.GetProperties();

            foreach (var prop in props)
            {
                schema.Fields.Add(new TableFieldSchema() {
                    Description = prop.Name,
                    Name = prop.Name,
                    Type = GetBQType(prop.PropertyType)
                });
            }

            return schema;
        }
       
        private object Parse(string type, string value) {

            if (value == null) return null;

            switch (type)
            {
                case "STRING":
                    return value;
                case "INTEGER":
                    return int.Parse(value, CultureInfo.InvariantCulture);
                case "FLOAT":
                    return double.Parse(value, CultureInfo.InvariantCulture);
                case "BOOLEAN":
                    return bool.Parse(value);
                case "TIMESTAMP":
                    return DateTime.Parse(value);
                case "RECORD":
                    throw new InvalidOperationException("Deserializer can't need support record type.");
                default:
                    throw new InvalidOperationException("UNKNOWN TYPE:" + type);
            }
        }
        private string GetBQType(Type t)
        {
            if (t.Equals(typeof(int))) return "INTEGER";
            else if (t.Equals(typeof(double))) return "FLOAT";
            else if (t.Equals(typeof(bool))) return "BOOLEAN";
            else if (t.Equals(typeof(DateTime))) return "TIMESTAMP";
            else if (t.Equals(typeof(string))) return "STRING";
            return "RECORD";
        }
        private IDictionary<string, object> AsDictionary(object source)
        {
            return source.GetType().GetProperties().ToDictionary
            (
                propInfo => propInfo.Name,
                propInfo => propInfo.GetValue(source, null)
            );

        }
       
    }
}
