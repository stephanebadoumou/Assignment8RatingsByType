using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Newtonsoft.Json;

using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
namespace RatingsByTypeTriggerAssignment7
{
    public class CompaniesMerging //Book
    {
        public string itemId;
        public string company;
        public string description;
        public string type;
        public int rating;
        public string lastInstanceOfWord;
    }
    public class Function
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        public async Task<List<CompaniesMerging>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "RatingsByType");
            List<CompaniesMerging> books = new List<CompaniesMerging>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;
            #region Single Record
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    CompaniesMerging myBook = JsonConvert.DeserializeObject<CompaniesMerging>(myDoc.ToJson());
                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {"type", new AttributeValue {S = myBook.type} }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "count",
                                new AttributeValueUpdate{ Action = "ADD", Value = new AttributeValue { N = "1"} }
                            },
                            {
                                "totalRating",
                                new AttributeValueUpdate{ Action = "ADD", Value = new AttributeValue{ N = myBook.rating.ToString()} }
                            },/*
                            {
                                "averageRating",
                                new AttributeValueUpdate{ Action = "ADD", Value = new AttributeValue{ N = ""} }
                            },  */                     
                        },

                    };
                    await client.UpdateItemAsync(request);
                }
            }
            #endregion
            return books;
        }
    }
}
