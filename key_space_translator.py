from redis import Redis
from json import loads
from dict2redis import Dict2redis

def get_personal_info(guid):
    """ returns personal information.
        client_id is int if present.
        allocated_vouchers is list if present.
    """
    if not guid:
        return {}
    key = "_".join((guid, "pi"))
    data = r.hgetall(key) or {}
    data["guid"] = guid
    data.setdefault("partner_id", data.get("partnerId",""))
    if "client_id" in data:
        data["client_id"] = int(data.get("client_id", 0))
    if "allocated_vouchers" in data:
        vs = data["allocated_vouchers"].split(",")
        data["allocated_vouchers"] = list(filter(None, vs))
    return data

def dump_personal_info_generator():
    for key in r.scan_iter(match="*_pi"):
        result = {"key": key}
        result.update(get_personal_info(key.split("_")[0]))
        yield result

def dump_vouchers_generator():
    for key in r.scan_iter(match="*_ri"):
        yield loads(r.get(key))

r = Redis("localhost", 6379, decode_responses=True, db=0)

pic = Dict2redis("guid", "2h:pi")

pis = dump_personal_info_generator()
for pi in pis:
    pi.pop("key")
    pic.save(pi)