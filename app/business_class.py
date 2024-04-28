import msgspec 
import json 
class Entity(msgspec.Struct):
    """A name linked to an address and an index value by a type of relationship.
    For example, the name and address of a person, where the index is the file number of a company, and the type is "manager"
    """
    id : int
    entity_type : str 
    link_type : str|None 
    name : str 
    street : str
    city: str 
    state : str 
    postal_code : str 
    details : str  
    index_type : str 
    index_value : str  
    is_primary: int
    link_to : str | None = None
    primary_type : str | None = None
    
    def to_dict(self):
        d = {f: getattr(self, f) for f in self.__struct_fields__}
        # d['details'] = json.loads(self.details)
        return d
    
    def export_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}