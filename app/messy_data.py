import pandas as pd 
import probablepeople as pp
import usaddress
from scourgify import normalize_address_record
import us
from uszipcode import SearchEngine
engine = SearchEngine()

from usps_suffixes import suffixes

state_names = [s.name for s in us.states.STATES]
state_abbr = [s.abbr for s in us.states.STATES]

def remove_parts(source:str, parts:list):
    for p in parts:
        if p is not None:
            source = source.replace(p, '').strip()
    return source 


def clean_name(name):
    try:
        relevant = ['GivenName', 'MiddleInitial', 'Surname', 'SuffixGenerational']
        
        if " SAME " in name or name[-5:] == " SAME":
            name = name.replace(" SAME", "")
            
        parts = pp.parse(name)
        constructed = []
        included = []

        for r in relevant:
            for p in parts: 
                if p[1] == r:
                    constructed.append(p[0].replace('.', '').replace(',', ''))
                    included.append(p[1])
        if 'GivenName' in included and 'Surname' in included:
            return ' '.join(constructed).upper()
        else:
            return name.upper()
    except Exception as e:
        if name is not None:
            return name.upper()
        

def clean_street(raw:str):
    relevant = ['AddressNumber', 'StreetNamePreDirectional', 'StreetName', 'StreetNamePostType', 'OccupancyType', 'OccupancyIdentifier', 'SubaddressType', 'SubaddressIdentifier', 'PlaceName', 'ZipCode']
    try:
        tags = usaddress.tag(raw.upper())[0]
        if 'AddressNumber' in tags:
            if "StreetNamePostType" in tags:
                tags["StreetNamePostType"] = suffixes.get(tags["StreetNamePostType"])
            label_parts = []
            for t in tags:
                if t in relevant and tags[t] is not None:
                    label_parts.append(tags[t].replace('.', '').upper())
            label = " ".join(label_parts)
            normalized = normalize_address_record(label)
            label = normalized['address_line_1']
            if normalized['address_line_2'] is not None:
                label += f" {normalized['address_line_2']}"
            return label
        else:
            return raw.upper()
    except Exception as e:
        # print(e)
        if raw is not None:
            return raw.upper()    
    
            
def extract_name_and_address(raw:str)->dict:
    excluded = ["INVOLUNTARY", "VACANT", "VACATED", "SOLE OFFICER", "None", "SAME ", "REVOKED ", " DISSOLUTION", "UNACCEPTABLE ", "MERGED ", "WITHDRAWN"]
    try:
        for e in excluded: 
            if e in raw:
                raise Exception("excluded name")
            
        raw = raw.strip()
        if raw[:3] == '"I"':
            raw = raw[3:]
            
        tags = usaddress.tag(raw)
        tags = dict(tags[0])
        
        record = {
            "name": clean_name(tags['Recipient']) if 'Recipient' in tags else None,
            "city": tags['PlaceName'] if 'PlaceName' in tags else None,
            "postal_code": tags['ZipCode'][:5] if 'ZipCode' in tags else None,
            "state": tags['StateName'] if 'StateName' in tags and tags['StateName'] in state_abbr else None        
        }
        
        if record['name'] is None and 'BuildingName' in tags: 
            record['name'] = clean_name(tags['BuildingName'])
            
        if record['name'] is not None and (record['city'] is None and record['postal_code'] is None and record['state'] is None):
            return record 
        
        if isinstance(record['postal_code'], str):
            zipcode = engine.by_zipcode(int(record['postal_code']))
            if zipcode:
                record['state'] = zipcode.state
                record['city'] = zipcode.major_city.upper()
        
        mostly_street = remove_parts(raw, [record['name'], record['postal_code'], record['city'], record['state']])
        
        if record['state'] in state_names or record['state'] in state_abbr:
            record['street'] = clean_street(mostly_street)
        else:
            record['street'] = mostly_street
        if record['name'] is None: 
            return {
                "name": raw.upper(), 
                "city": None, 
                "postal_code": None, 
                "state": None
            }
        return record 

    except Exception as e:
        pass 
        # print(raw,e)
        
        
def extract_names_and_addresses(df, index_field, extract_field, type):
    print(index_field)
    records = []
    errors = []
    for row in df.to_dict('records'):
        if pd.notna(row[extract_field]):
            try:
                record = {
                    index_field: row[index_field],
                    extract_field: row[extract_field], 
                    "type": type 
                }
                parts = extract_name_and_address(row[extract_field])
                if parts is not None:
                    record.update(parts)
                records.append(record)
                
            except Exception as e:
                errors.append((row, e))
                pass 

        
    return pd.DataFrame(records)