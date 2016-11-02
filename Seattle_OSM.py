
# coding: utf-8

"""

This code wrangles and analyses OpenStreetMap data for the Seattle - Washington region.

The data was downloaded from this link: https://mapzen.com/data/metro-extracts/metro/seattle_washington/

"""

import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
from collections import defaultdict

OSMFILE = "seattle_washington.osm"
"""string: OSM input file."""

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
"""regex: Regular Expression for Street Names."""
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
"""regex: Regular Expression for Problematic Chars."""

mapping = { "St": "Street",
            "St.": "Street",
            "Rd": "Road",
            "Rd.": "Road",
            "Ave": "Avenue",
            "Hwy": "Highway",
            "NE": "Northeast",
            "N.E.": "Northeast",
            "NW": "Northwest",
            "N.W.": "Northwest",
            "S": "South",
            "SE": "Southeast",
            "S.E.": "Southeast",
            "SW": "Southwest",
            "S.W.": "Southwest",
            "W" : "West",
            "E" : "East"
            }
"""dictionary: Maps street name type abbreviations to the full street name type."""

def is_street_name(elem):
    """

    This function verifies if the 'k' attribute of the XML element is a street name.

    Args:
        param1 (xml): XML Element

    Returns:
        bool: The return value. True for success, False otherwise.

    """
    return (elem.attrib['k'] == "addr:street")

def update_name(name, mapping):
    """

    This function updates a street name type to a new value defined in the mapping variable. If the type is not
    included in the mapping the name is not updated.

    Args:
        param1 (str): street name.
        param2 (dictionary): street name type dictionary.

    Returns:
        str: The new name.

    """
    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        for key, value in mapping.iteritems():
            if street_type == key:
                name = name.replace(key,value)

    return name

def shape_element(element):
    """

    This function converts an XML element to a dictionary.

    Args:
        param1 (xml): XML Element.
        
    Returns:
        dictionary: A dictionary with the XML elements.

    """
    node = {}
    if element.tag == "node" or element.tag == "way" :
        lat = str(element.get("lat"))
        lon = str(element.get("lon"))
        try:
            node["pos"] = [float(lat),float(lon)]
        except:
            pass
        created = {}
        created["changeset"] = element.get("changeset")
        created["user"] = element.get("user")
        created["version"] = element.get("version")
        created["uid"] = element.get("uid")
        created["timestamp"] = element.get("timestamp")
        node["created"] = created
        node["visible"] = element.get("visible")
        node["type"] = element.tag
        node["id"] = element.get("id")
        
        ##Parse address elements
        address = {}
        for subelement in element.iter("tag"):
            k_element = subelement.get("k")
            v_element = subelement.get("v")
            if not problemchars.match(k_element):
                if k_element.startswith("addr:"):
                    if is_street_name(subelement):
                        v_element = update_name(v_element,mapping)
                    k_elements = k_element.split(":")
                    if(len(k_elements) < 3):
                        address[k_elements[1]] = v_element
                else:
                    node[k_element] = v_element
        if(bool(address)):
            node["address"] = address
            
        if element.tag == "way":
            node_refs = []
            for subelement in element.iter("nd"):
                node_refs.append(subelement.get("ref"))
            node["node_refs"] = node_refs
        
        return node
    else:
        return None

def process_map(file_in, db_table):
    """

    This file inserts a JSON input to a MongoDB table.

    Args:
        param1 (json): JSON input.
        param2 (MongoDB table): MongoDB table.

    """
    data = []
    i = 0
    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        if el != None:
            data.append(el)
            i = i + 1
            #Insert every 10,000 records to the database
            if i == 10000:
                db_table.insert_many(data)
                #Empty data list and restart count
                data[:] = []
                i = 0
    #Insert rest of the data list to the database
    db_table.insert_many(data)

def get_db():
    """

    This function returns the MongoDB instance.

    Returns:
        MongoDB: The MongoDB instance.

    """
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.seattle
    return db


# The code below will use the functions declared above and load the OSM file and save it to the MongoDB database

db = get_db()

db.seattle_data.drop()

process_map(OSMFILE, db.seattle_data)


# In the query below we will count the number of documents that we imported:

db.seattle_data.find().count()  


# Now we will count the number of nodes and ways:

db.seattle_data.find({"type":"node"}).count()

db.seattle_data.find({"type":"way"}).count()

# We could also investigate some other characteristics of the dataset, such as the top 10 types of parking structures:

match = {"$match":{"amenity":{"$eq":"parking"}}}
group = {"$group":{"_id":"$parking", "count":{"$sum":1}}}
sort = {"$sort":{"count": -1}}
limit = {"$limit" : 10}
result = db.seattle_data.aggregate([match, group, sort, limit])

for r in result:
    pprint.pprint(r)

# Or the top 10 cities with the highest number of records:

match = {"$match":{"address.city":{"$exists":1}}}
group = {"$group":{"_id":"$address.city", "count":{"$sum":1}}}
sort = {"$sort":{"count": -1}}
limit = {"$limit" : 10}
result = db.seattle_data.aggregate([match, group, sort, limit])

for r in result:
    pprint.pprint(r)

# A small portion of this dataset is located in Canada. In the next steps we are going to investigate if the records correctly reference the country in which they are located. We will start by grouping the country field and seeing which values are being used:

group = {"$group":{"_id":"$address.country", "count":{"$sum":1}}}
limit = {"$limit": 100}
result = db.seattle_data.aggregate([group, limit])

for r in result:
    pprint.pprint(r)

# The query above shows that the majority of the records do not include country information. As an alternative we could investigate records which have a province or state record:

group = {"$group":{"_id":"$address.province", "count":{"$sum":1}}}
limit = {"$limit": 100}
result = db.seattle_data.aggregate([group, limit])

for r in result:
    pprint.pprint(r)

# The query results above shows that there are a few records without a country which are located in British Columbia. We will set the country as Canada for those records an standardize all of them to use the same convention for the province name. After that we will set all of those records to have 'CA' as the country.

db.seattle_data.update_many({"address.province" : {"$eq" : "British Columbia"}}, {"$set" : {"address.province" : "BC" } })
db.seattle_data.update_many({"address.province" : {"$eq" : "BC"}}, {"$set" : {"address.country" : "CA" } })

# Now we will verify how the values for the state record. The code below shows us that several fields have street names on their state field which indicates incorrect data entry.

group = {"$group":{"_id":"$address.state", "count":{"$sum":1}}}
limit = {"$limit": 100}
result = db.seattle_data.aggregate([group, limit])

for r in result:
    pprint.pprint(r)


# We will update the records which has Washington, washington or wa the state to use the WA value. After that, we will update the country in the records located in Washington State.

db.seattle_data.update_many({"$or" : [{"address.state" : {"$eq" : "washington"}},{"address.state" : {"$eq" : "Washington"}}          ,{"address.state" : {"$eq" : "wa"}}, {"address.state" : {"$eq" : "Wa"}}           ,{"address.state" : {"$eq" : "WA."}}, {"address.state" : {"$eq" : "WA - Washington"}}                                     ]}, {"$set" : {"address.state" : "WA" } })
db.seattle_data.update_many({"address.state" : {"$eq" : "WA"}}, {"$set" : {"address.country" : "US" } })


# Verifying that the updates increased the number of records for US and Canada

group = {"$group":{"_id":"$address.country", "count":{"$sum":1}}}
limit = {"$limit": 100}
result = db.seattle_data.aggregate([group, limit])

for r in result:
    pprint.pprint(r)

# Using a regular expression to find records which do not use the 5 digit rule for post codes (in the US)

match1 = {"$match":{"address.postcode":{"$exists":"1"}, "address.country" : {"$eq" : "US"}}}
match2 = {"$match":{"address.postcode":{"$not" : re.compile("^\d{5}$")}}}
limit = {"$limit" : 10}
result = db.seattle_data.aggregate([match1, match2, limit])

for r in result:
   pprint.pprint(r["address"]["postcode"])

group = {"$group":{"_id":"1", "count":{"$sum":1}}}

result = db.seattle_data.aggregate([match1, match2, group, limit])

# Getting the number of records
for r in result:
    pprint.pprint(r["count"])

# Fixing the records with more than 5 digits

result = db.seattle_data.aggregate([match1, match2])

from bson.objectid import ObjectId

for r in result:
    object_id = ObjectId(r["_id"])
    post_code = r["address"]["postcode"][:5]
    db.seattle_data.update_one({"_id" : {"$eq" : object_id}},
                                {"$set" : {"address.postcode" : post_code}})

# Verifying that all records where fixed

result = db.seattle_data.aggregate([match1, match2, group, limit])

for r in result:
    pprint.pprint(r["count"])

