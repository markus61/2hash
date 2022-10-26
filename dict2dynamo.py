from boto3 import resource
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from pickle import loads, dumps
from deepdiff import DeepDiff

class Dict2dynamo():
    dynamodb = resource('dynamodb', endpoint_url="http://localhost:8000")

    def __key(self, k) -> str:
        """delivers the lookup key for redis.

        Args:
            k (str|dict): the individual part of the key as str or a dictionary containing self.key

        Raises:
            ValueError: data needs key

        Returns:
            str: redis key
        """

        k = k.get(self.key) if isinstance(k, dict) else k
        if not k:
            raise ValueError(
                "data needs a key '{}' with value not None.".format(self.key))
        return ":".join((self.prefix, k))

    def __write(self, d: dict) -> bool:
        """writes hash to dynamodb. key comes from payload 'd'. pickles to '_2h_' if d has non str values.
        returns 1 for success, 0 for failure

        Args:
            d (dict): payload to be stored to table, defaults to None

        Returns:
            bool: True if write was successful
        """
        return self.table.put_item(Item=d)

    def __init__(self, key: str = "", table: str = "") -> None:
        """initialise -  defines table for this container. key is made up by 3 parts <prefix>:<key>:<individual_id>

        Args:
            key (str): sets second slice of key.
            prefix (str, optional): first slice of key. Defaults to "2h".
        """
        self.key = key
        self.table_name = table
        self.table = self.dynamodb.Table(table)

    def load(self, id):
        """use id to find and load data

        Args:
            id (str): third slice in redis key. verify __init__().

        Returns:
            dict: the payload
        """
        try:
            response = self.table.get_item(
                Key={self.key: id}
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response.get("Item", {})

    def save(self, d: dict) -> dict:
        """saves the payload to redis

        Args:
            d (dict): the payload. must contain an id (value for self.key in p)

        Returns:
            dict: returns the payload unchanged
        """
        return self.__write(d)

    def update(self, p: dict) -> dict:
        """update data in redis and return the full datatset

        Args:
            p (dict): an update to data in redis identified by id in p.

        Returns:
            dict: data available for the id in p.
        """
        self.table.update_item(
            Key={
                self.key: p[self.key]
            },
            UpdateExpression='SET age = :val1',
            ExpressionAttributeValues={
                ':val1': 26
            }
        )        
        return d

    def ids(self):
        """gather all ids for this storage container from redis

        Yields:
            str: utf-8 decoded id
        """
        pass 

    def find(self, f: str, p: str = "") -> dict:
        """search for data in any field that is not pickled and yield the corresponding hashes. 
        if a hash does not have a key 'field', this record is skipped.

        Args:
            f (str): the field to look for.
            p (str, optional): if given, the field value is compared to p (if p in value). 
              in case of no match, the record is skipped. Defaults to "".

        Returns:
            dict: [description]

        Yields:
            Iterator[dict]: whole data if has field and p is in field value.
        """
        try:
            response = self.table.query(
                IndexName=f,
                KeyConditionExpression=Key(f).eq(p)
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            for item in response.get("Items", []):
                yield self.load(item.get("guid"))

    def all(self):
        """all data

        Yields:
            dict: the payload
        """
        scan_kwargs = {
        }

        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = self.table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                yield item

            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None



from dict2redis import Dict2redis

s = Dict2redis("guid", "2h:pi")
d = Dict2dynamo("guid", "personal_info")

for item  in d.field("employeeEmail", "meingutschein2021+prod001new@gmail.com"):
    print(item)




"""

alldicts = s.all()
for i in alldicts:
    d.save(i)

items = d.all()
for i in items:
    #r = s.load(i.get("guid"))
    print(i)


#s.save(data)

data = {
    "id": str(uuid4()),
    "payload": "worth saving!",
    "integer": 10,
    "dict": {"listening_to": "deep purple"}
}

if s.save(data):
    print("apparently saved something somewehere.")
else:
    print("tss. get yourself another hobby.")

d = s.load(data)

p = {
    "id": d["id"],
    "markus": "geht jetzt schlafen."
}

print(s.update(p))
"""
