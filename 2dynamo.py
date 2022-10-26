from boto3 import resource
from uuid import uuid4
from pickle import loads, dumps
from uu import encode, decode


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

        d["_2h_"] = encode(dumps(d))
        return 

    def __init__(self, table: str = "") -> None:
        """initialise -  defines table for this container. key is made up by 3 parts <prefix>:<key>:<individual_id>

        Args:
            key (str): sets second slice of key.
            prefix (str, optional): first slice of key. Defaults to "2h".
        """
        self.table_name = table
        self.table = self.dynamodb.Table(table)

    def load(self, id):
        """use id to find and load data

        Args:
            id (str): third slice in redis key. verify __init__().

        Returns:
            dict: the payload
        """
        x = self.r.hgetall(self.__key(id))
        if b'_2h_' in x.keys():
            return loads(x[b'_2h_'])
        d = {}
        for k, v in x.items():
            d[k.decode("utf-8")] = v.decode("utf-8")
        return d

    def save(self, d: dict) -> dict:
        """saves the payload to redis

        Args:
            d (dict): the payload. must contain an id (value for self.key in p)

        Returns:
            dict: returns the payload unchanged
        """
        self.r.delete(self.__key(d))
        return self.__write(d)

    def update(self, p: dict) -> dict:
        """update data in redis and return the full datatset

        Args:
            p (dict): an update to data in redis identified by id in p.

        Returns:
            dict: data available for the id in p.
        """
        d = self.load(p)
        d.update(p)
        self.__write(d)
        return d

    def ids(self):
        """gather all ids for this storage container from redis

        Yields:
            str: utf-8 decoded id
        """
        for k in self.r.scan_iter("{}:*".format(self.prefix), count=1):
            yield k.decode("utf-8")[len(self.prefix)+1:]

    def field(self, f: str, p: str = "") -> dict:
        """search for data in any field that is not pickled and yields the corresponding hashes. if a hash does not have a key 'field', this record is skipped.

        Args:
            f (str): the field to look for.
            p (str, optional): if given, the field value is compared to p (if p in value). in case of no match, the record is skipped. Defaults to "".

        Returns:
            dict: [description]

        Yields:
            Iterator[dict]: whole data if has field and p is in field value.
        """
        for id in self.ids():
            r = self.r.hget(self.__key(id), f)
            if not r:
                continue
            r = r.decode("utf-8")
            if not p or p in r:
                yield self.load(id)

    def all(self):
        """all data

        Yields:
            dict: the payload
        """
        for id in self.ids():
            yield self.load(id)


s = Dict2redis("id")

for f in s.field("markus", "schlafen"):
    print(f)

data = {
    "id": str(uuid4()),
    "integer": 10,
    "new_field": "tomato"
}

# s.save(data)

"""
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
