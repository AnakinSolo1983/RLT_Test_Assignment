import motor.motor_asyncio
import datetime
import asyncio

#-------

client = motor.motor_asyncio.AsyncIOMotorClient("localhost", 27017)
db = client["newdb"]
collection = db["sample_collection"]

time_interval = {
    "hour": "%Y-%m-%dT%H:00:00",
    "day": "%Y-%m-%dT00:00:00",
    "month": "%Y-%m-01T00:00:00",
}

# This function takes three parameters: dt_from (start date and time), dt_upto (end date and time), and group_type (time interval). 
# It performs the aggregation operation on the collection based on the given parameters.
async def aggregate_data(dt_from,dt_upto,group_type):

    start = datetime.datetime.fromisoformat(dt_from)
    end = datetime.datetime.fromisoformat(dt_upto)
    format = time_interval[group_type]
    pipeline = [
        
        # Filters the documents in the collection based on the date interval specified by dt_from and dt_upto.
        {"$match": {"dt": {"$gte": start, "$lte": end}}},
        # Groups the remaining documents by the specified time interval format and calculates the sum of the value field.
        {"$group": 
           { 
            "_id": {"$dateToString": {"format": format, "date": "$dt"}},
            "total": {"$sum": "$value"}
           } 
        },
        # Sorts the aggregated data by the _id field in ascending order.
        {"$sort": {"_id": 1}},
    ]
    
    cursor = collection.aggregate(pipeline)
    curs = await cursor.to_list(length=None)
    
    labels = []
    data = []
    for document in curs:

        labels.append(document['_id'])
        data.append(document['total'])

    result_data = []
    result_labels = []

    current = start
    while current <= end:

        curiso = current.isoformat()
        result_labels.append(curiso)

        # The code checks if there are any missing data points in the specified date range. 
        # If a data point is missing for a particular time interval, it appends a value of 0 to the result_data list.
        if curiso not in labels:

            result_data.append(0)
        
        else:

            ind = labels.index(curiso)
            result_data.append(data[ind])

        if group_type == 'hour':

            current += datetime.timedelta(hours=1)

        elif group_type == 'day':

            current += datetime.timedelta(days=1)         
    
        elif group_type == 'month':

            current = current.replace(day=1) + datetime.timedelta(days=32)
            current = current.replace(day=1)

    output = {"dataset": result_data, "labels": result_labels}
    return output
