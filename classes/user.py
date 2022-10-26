class User(object):
    __slots__ = [
        "registertime",
        "userid",
        "regionid",
        "gender"
    ]
    
    @staticmethod
    def get_schema():
        with open('./avro/user.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, registertime, userid, regionid, gender):
        self.registertime = registertime
        self.userid       = userid
        self.regionid     = regionid
        self.gender       = gender

    @staticmethod
    def dict_to_user(obj, ctx=None):
        return User(
                obj['registertime'],
                obj['userid'],    
                obj['regionid'], 
                obj['gender']   
            )

    @staticmethod
    def user_to_dict(user, ctx=None):
        return User.to_dict(user)

    def to_dict(self):
        return dict(
                    registertime = self.registertime,
                    userid       = self.userid,
                    regionid     = self.regionid,
                    gender       = self.gender
                )