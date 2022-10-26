class UserMask(object):
    __slots__ = [
        "registertime",
        "userid",
        "regionid"
    ]
    
    @staticmethod
    def get_schema():
        with open('./avro/usermask.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, registertime, userid, regionid):
        self.registertime = registertime
        self.userid       = userid
        self.regionid     = regionid

    @staticmethod
    def dict_to_usermask(obj, ctx=None):
        return UserMask(
                obj['registertime'],
                obj['userid'],    
                obj['regionid']  
            )

    @staticmethod
    def usermask_to_dict(user, ctx=None):
        return UserMask.to_dict(user)

    def to_dict(self):
        return dict(
                    registertime = self.registertime,
                    userid       = self.userid,
                    regionid     = self.regionid
                )